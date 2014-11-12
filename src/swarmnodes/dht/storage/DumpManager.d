/*******************************************************************************

    Memory dumps manager

    copyright:      Copyright (c) 2013 sociomantic labs. All rights reserved

    authors:        Leandro Lucarella

    This module manages all the dumping/loading related operations for channels.
    It handles opening files, doing the actual dumping, renaming, backing up,
    etc.

*******************************************************************************/

module swarmnodes.dht.storage.DumpManager;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.common.kvstore.storage.KVStorageEngine;

private import swarmnodes.common.kvstore.storage.IStepIterator;

private import swarmnodes.dht.storage.DumpFile;

private import swarm.dht.DhtHash;

private import ocean.core.Array : copy, startsWith;

private import ocean.io.FilePath;

private import ocean.util.log.StaticTrace;

private import tango.time.StopWatch;

private import ocean.io.Stdout;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("swarmnodes.dht.storage.DumpManager");
}



/*******************************************************************************

    Dump manager class.

    This class takes care of all file operations for the channels (that is:
    loading, dumping, deleting), including opening the files that channels
    should use to dump and load. The manager is careful to maintain the
    integrity of dump files, such that a partially written file will never exist
    with the standard file suffix (they are written to remporary files first).

*******************************************************************************/

public class DumpManager
{
    /***************************************************************************

        Enum of out-of-range records handling modes.

    ***************************************************************************/

    public enum OutOfRangeHandling
    {
        Load,   // out-of-range records loaded (logged as trace)
        Fatal,  // quit on finding an out-of-range record (logged as fatal)
        Ignore  // out-of-range records not loaded (logged as warn)
    }


    /***************************************************************************

        Callback type used to create a new storage engine instance.

        Params:
            id = name of the channel than needs to be created

    ***************************************************************************/

    public alias KVStorageEngine delegate ( char[] id ) NewChannelCb;


    /***************************************************************************

        Output buffered direct I/O file, used to dump the channels.

    ***************************************************************************/

    private const ChannelDumper output;


    /***************************************************************************

        Input buffered direct I/O file, used to load the channel dumps.

    ***************************************************************************/

    private const ChannelLoader input;


    /***************************************************************************

        File paths, re-used for various file operations

    ***************************************************************************/

    private const FilePath path;

    private const FilePath dst_path;


    /***************************************************************************

        Root directory used to look for files and write dump files.

    ***************************************************************************/

    private const FilePath root_dir;


    /***************************************************************************

        Delete directory, inside root directory, where deleted dump files are
        moved to.

    ***************************************************************************/

    private const FilePath delete_dir;


    /***************************************************************************

        KVStorageEngine iterator to use while dumping.

    ***************************************************************************/

    private const IStepIterator iterator;


    /***************************************************************************

       Strings used for db iteration during load of a dump file.

    ***************************************************************************/

    private char[] load_key, load_value;


    /***************************************************************************

        Determines how out-of-range records (i.e. those whose keys are not in
        the range of hashes supported by the node) are handled (see enum, above)

    ***************************************************************************/

    private const OutOfRangeHandling out_of_range_handling;


    /***************************************************************************

        Constructor.

        Params:
            root_dir = root directory used to look for files and write dumps.
            iterator = KVStorageEngine iterator instance to use for dumping.
            out_of_range_handling = determines how out-of-range records (i.e.
                those whose keys are not in the range of hashes supported by the
                node) are handled (see enum, above)
            disable_direct_io = determines if regular buffered I/O (true) or
                direct I/O is used (false). Regular I/O is only useful for
                testing, because direct I/O imposes some restrictions over the
                type of filesystem that can be used.

    ***************************************************************************/

    public this ( FilePath root_dir, IStepIterator iterator,
        OutOfRangeHandling out_of_range_handling, bool disable_direct_io )
    {
        this.root_dir = new FilePath(root_dir.toString());
        this.delete_dir = new FilePath(root_dir.toString());
        this.delete_dir.append("deleted");

        // ensure 'deleted' folder exists
        if ( !this.delete_dir.exists )
        {
            this.delete_dir.create();
        }

        this.iterator = iterator;

        this.path = new FilePath;
        this.dst_path = new FilePath;

        auto buffer = new ubyte[IOBufferSize];
        this.output = new ChannelDumper(buffer, NewFileSuffix,
                disable_direct_io);
        this.input = new ChannelLoader(buffer, disable_direct_io);

        this.out_of_range_handling = out_of_range_handling;
    }


    /***************************************************************************

        Dump a channel in an "atomic" way.

        Dump a channel with name id using the dump_channel callback to do the
        actual dumping. The dump is performed in a temporary file and only
        renamed to the standard dump file name if the dump finishes
        successfully.

        Params:
            storage = storage engine instance to dump
            verbose = true if a progress indication must be shown.

        See_Also:
            rotateDumpFile() for details on the rotation algorithm.

    ***************************************************************************/

    public void dump ( KVStorageEngine storage, bool verbose = false )
    {

        // Make the dump and close the file after leaving this scope
        {
            buildFilePath(this.root_dir, this.path, storage.id).cat(".");
            this.output.open(this.path.toString());
            scope (exit) this.output.close();

            this.dumpChannel(storage, this.output, verbose);
        }

        // Atomically move dump.new -> dump
        rotateDumpFile(this.output.path, storage.id, this.root_dir, this.path,
            this.dst_path);

        log.info("Finished channel dump, {} bytes written",
            buildFilePath(this.root_dir, this.path, storage.id).fileSize());
    }


    /***************************************************************************

        Writes the contents of a storage engine to a file.

        THIS CODE IS A TRANSITION BETWEEN AND OLDER DUMP FORMAT AND A NEW ONE.

        We want to move to a dump format that doesn't need to seek in the file
        to be written. This means eliminating the number of records at the
        beginning of the file. Instead, and to avoid this problem in the future,
        this header will be used to store a dump file format version number. The
        first version will be 0, so for the transition, since 0 is not a valid
        key and not a valid number of records field (channels with no records
        don't get dumped at all), we can detect if a file is in an old or a new
        format (0 new, != old) and act accordingly.

        If the header is 0 we'll ignore this field and read the file until we
        find a 0 key (cast(char[]) null really, since right now keys are being
        written as ASCII using a whole array), that flags the end of the file
        (this marked is added for safety reasons, as direct I/O files need to
        write whole blocks, it might happen that the dump file is bigger than it
        should be). If is != 0, we read the number of records, and read at most
        this number of records, reporting if there is a mismatch between read
        records and the number of records indicated in the header.

        Once we are sure we don't have any dump files in the old format, we can
        avoid the special casing and just treat the header as a version number,
        which at this point will be incremented to one. From now on, any change
        in the dump file format can be detected just by checking the version
        number.

        Params:
            storage = KVStorageEngine to dump
            output = file where to write the channel dump
            verbose = if true, print progrss on the dumping

    ***************************************************************************/

    private void dumpChannel ( KVStorageEngine storage, ChannelDumper output,
        bool verbose = false )
    {
        log.info("Dumping channel '{}' to disk", storage.id);
        if ( verbose )
        {
            Stderr.format("Dumping channel '{}' to disk", storage.id)
                .clearline.newline;
        }

        scope progress_manager = new ProgressManager("Dumped channel",
                storage.id, storage.num_records, verbose);

        // Write records
        iterator.setStorage(storage);
        for ( storage.getAll(this.iterator); !this.iterator.lastKey();
                this.iterator.next() )
        {
            // TODO: handle case where out of disk space

            output.write(this.iterator.key, this.iterator.value);

            progress_manager.progress(1);
        }

        log.info("Finished dumping channel '{}' to disk, took {}s, "
            "wrote {} records, {} records in channel",
            storage.id, progress_manager.elapsed, progress_manager.current,
            storage.num_records);
    }


    /***************************************************************************

        Load channel dump files found in this.root_dir.

        We look for dump files in the directory and load the channel when
        a valid file is found. Other files in the data directory trigger a
        warning message (a special warning for partially written dump files).

        Params:
            new_channel = callback to use to create new channels

    ***************************************************************************/

    public void loadChannels ( NewChannelCb new_channel )
    {
        foreach ( info; this.root_dir )
        {
            this.path.set(this.root_dir);
            this.path.append(info.name);

            if ( info.folder )
            {
                // Having the delete_dir there is, of course, fine
                if ( this.path == this.delete_dir )
                    continue;

                log.warn("Ignoring subdirectory '{}' in data directory {}",
                        info.name, this.root_dir.toString);
                Stderr.formatln("Ignoring subdirectory '{}' in data directory {}",
                        info.name, this.root_dir.toString);
                continue;
            }

            if ( this.path.suffix() == DumpFileSuffix )
            {
                // We don't reuse this.path for the complete path to avoid
                // conflicts between buffers
                buildFilePath(this.root_dir, this.dst_path, this.path.name);

                this.input.open(this.dst_path.toString());
                scope (exit) this.input.close();

                auto channel = new_channel(this.dst_path.name.dup);
                this.loadChannel(channel, this.input, this.out_of_range_handling);
            }
            else if ( this.path.suffix() == NewFileSuffix )
            {
                log.warn("{}: Unfinished dump file found while scanning "
                        "directory '{}', the program was probably "
                        "restarted uncleanly and data might be old",
                        this.path.file, this.root_dir.toString);
                Stderr.formatln("{}: Unfinished dump file found while scanning "
                        "directory '{}', the program was probably "
                        "restarted uncleanly and data might be old",
                        this.path.file, this.root_dir.toString);
            }
            else
            {
                log.warn("{}: Ignoring file while scanning directory '{}' "
                        "(no '{}' suffix)", this.path.file,
                        this.root_dir.toString, DumpFileSuffix);
                Stderr.formatln("{}: Ignoring file while scanning directory "
                        "'{}' (no '{}' suffix)", this.path.file,
                        this.root_dir.toString, DumpFileSuffix);
            }
        }
    }


    /***************************************************************************

        Loads data from a previously dumped image from a file.

        THIS CODE IS A TRANSITION BETWEEN AND OLDER DUMP FORMAT AND A NEW ONE.

        Please read the comment in dumpToStream() for details on the migration
        path.

        Params:
            storage = channel storage to load the dump to
            input = file from where to read the channel dump
            out_of_range_handling = out-of-range record handling mode

        Throws:
            if out_of_range_handling == Fatal and an out-of-range record is
            encountered while loading the input file

    ***************************************************************************/

    static private void loadChannel ( KVStorageEngine storage,
        ChannelLoaderBase input, OutOfRangeHandling out_of_range_handling )
    {
        log.info("Loading channel '{}' from disk", storage.id);
        Stderr.formatln("Loading channel '{}' from disk", storage.id);

        scope progress_manager = new ProgressManager("Loaded channel",
                    storage.id, input.length);

        // Just to avoid confusion, will go away after the transition.
        ulong num_records = input.file_format_version;
        if ( num_records > 0 )
        {
            log.info("File is in old, versionless format");
        }

        ulong records_read, invalid, out_of_range;
        foreach ( k, v; input )
        {
            records_read++;

            progress_manager.progress(k.length + v.length + (size_t.sizeof * 2));

            loadRecord(storage, k, v, out_of_range_handling,
                out_of_range, invalid);

            // This will go after the transition!
            if (num_records > 0 && records_read == num_records)
            {
                break;
            }
        }

        // This will go after the transition!
        if (num_records > 0 && num_records != records_read)
        {
            log.error("Number of records mismatch while loading dump "
                    "file. Expected {} records, got {}",
                    num_records, records_read);
            throw new Exception("Number of records mismatch while "
                    "loading dump file.");
        }

        void reportBadRecordCount ( ulong bad, char[] desc )
        {
            if ( bad )
            {
                auto percent_bad =
                    (cast(float)bad / cast(float)records_read) * 100.0;
                log.warn("Found {} {} ({}%) in channel '{}'", bad, desc,
                    percent_bad, storage.id);
                Stderr.red.formatln("Found {} {} ({}%) in channel '{}'",
                    bad, desc, percent_bad, storage.id).default_colour;
            }
        }

        reportBadRecordCount(out_of_range, "out-of-range keys");
        reportBadRecordCount(invalid, "invalid keys");

        log.info("Finished loading channel '{}' from disk, took {}s, "
            "read {} bytes (file size including padding is {} bytes), "
            "{} records in channel", storage.id, progress_manager.elapsed,
            progress_manager.current, progress_manager.maximum,
            storage.num_records);
    }


    /***************************************************************************

        Loads a record into the specified storage channel. Checks whether the
        record's key is valid and within the hash range of the storage engine.

        Params:
            storage = channel storage to load the record into
            key = record key
            val = record value
            out_of_range_handling = out-of-range record handling mode
            out_of_range = count of out-of-range records; incremented if key is
                outside of the node's hash range
            invalid = count of invalid keys; incremented if key is invalid

        Throws:
            if out_of_range_handling == Fatal and the record is out-of-range

    ***************************************************************************/

    static private void loadRecord ( KVStorageEngine storage, char[] key,
        char[] val, OutOfRangeHandling out_of_range_handling,
        ref ulong out_of_range, ref ulong invalid )
    {
        if ( !DhtHash.isHash(key) )
        {
            log.error("Encountered invalid non-hexadecimal key in channel '{}': "
                "{} -- ignored", storage.id, key);
            invalid++;
            return;
        }

        if ( storage.responsibleForKey(key) )
        {
            storage.put(key, val);
        }
        else
        {
            with ( OutOfRangeHandling ) switch ( out_of_range_handling )
            {
                case Load:
                    log.trace("Encountered out-of-range key in channel '{}': "
                        "{} -- loaded", storage.id, key);
                    storage.put(key, val);
                    out_of_range++;
                    return;

                case Fatal:
                    log.fatal("Encountered out-of-range key in channel '{}': "
                    "{} -- rejected", storage.id, key);
                    throw new Exception("Encountered out-of-range key in channel '"
                        ~ storage.id ~ "': " ~ key);

                case Ignore:
                    log.warn("Encountered out-of-range key in channel '{}': "
                        "{} -- ignored", storage.id, key);
                    out_of_range++;
                    return;

                default:
                    assert(false);
            }
        }
    }


    /***************************************************************************

        Virtually delete a channel dump file.

        What this method really does is move the old file into the 'deleted'
        sub-folder of the data directory. Files in this folder are not loaded by
        loadChannels().

        Params:
            id = name of the channel to delete

    ***************************************************************************/

    public void deleteChannel ( char[] id )
    {
        buildFilePath(this.root_dir, this.path, id);
        if ( this.path.exists )
        {
            buildFilePath(this.delete_dir, this.dst_path, id);
            // file -> deleted/file
            this.path.rename(this.dst_path);
        }
    }
}



/*******************************************************************************

    Unittest for DumpManager.loadChannel()

*******************************************************************************/

version ( UnitTest )
{
    private import ocean.core.Test : test, testThrown;
    private import ocean.io.device.MemoryDevice;
    private import tango.core.Exception : IOException;
    private import swarmnodes.common.kvstore.app.config.HashRangeConfig;

    private class DummyStorageEngine : KVStorageEngine
    {
        private uint count;

        this ( hash_t min = hash_t.min, hash_t max = hash_t.max )
        {
            super("test", new KVHashRange(min, max, new HashRangeConfig([])));
        }
        override typeof(this) put ( char[] key, char[] value )
        {
            this.count++;
            return this;
        }
        typeof(this) clear ( ) { return this; }
        typeof(this) close ( ) { return this; }
        ulong num_records ( ) { return this.count; }
        ulong num_bytes ( ) { return 0; }
    }

    private class DummyChannelLoader : ChannelLoaderBase
    {
        const size_t len;
        this ( ubyte[] data )
        {
            this.len = data.length;
            auto mem = new MemoryDevice;
            mem.write(data);
            mem.seek(0);
            super(mem);
        }
        protected ulong length_ ( ) { return this.len; }
    }
}


/*******************************************************************************

    Tests for handling file version and extra bytes at the end of the file.

*******************************************************************************/

unittest
{
    /***************************************************************************

        Calls DumpManager.loadChannel() with the provided input data.

        Params:
            data = data to load

        Returns:
            the number of records in the storage engine after loading

    ***************************************************************************/

    ulong testData ( ubyte[] data )
    {
        auto storage = new DummyStorageEngine;
        auto input = new DummyChannelLoader(data);
        input.open();

        DumpManager.loadChannel(storage, input,
            DumpManager.OutOfRangeHandling.Load);

        return storage.num_records;
    }

    ubyte[] versionless = [3,0,0,0,0,0,0,0]; // number of records
    ubyte[] version0 =    [0,0,0,0,0,0,0,0]; // version number
    ubyte[] data = [
        16,0,0,0,0,0,0,0, // key 1 len
        49,50,51,52,53,54,55,56,49,50,51,52,53,54,55,56, // key 1
        4,0,0,0,0,0,0,0, // value 1 len
        1,2,3,4, // value 1
        16,0,0,0,0,0,0,0, // key 2 len
        49,50,51,52,53,54,55,56,49,50,51,52,53,54,55,56, // key 2
        4,0,0,0,0,0,0,0, // value 2 len
        1,2,3,4, // value 2
        16,0,0,0,0,0,0,0, // key 3 len
        49,50,51,52,53,54,55,56,49,50,51,52,53,54,55,56, // key 3
        4,0,0,0,0,0,0,0, // value 3 len
        1,2,3,4 // value 3
    ];
    ubyte[] extra = [0,0,0,0,0,0,0,0];

    // versionless file with no extra bytes at end
    test!("==")(testData(versionless ~ data), 3);

    // versionless file with extra bytes at end
    test!("==")(testData(versionless ~ data ~ extra), 3);

    // version 0 file with no extra bytes at end
    testThrown!(IOException)(testData(version0 ~ data));

    // version 0 file with extra bytes at end
    test!("==")(testData(version0 ~ data ~ extra), 3);
}


/*******************************************************************************

    Tests for out-of-range record handling.

*******************************************************************************/

unittest
{
    /***************************************************************************

        Calls DumpManager.loadChannel() with the provided input data.

        Params:
            data = data to load
            out_of_range_handling = behaviour upon finding out-of-range records
            min = min hash of dummy node
            max = max hash of dummy node

        Returns:
            the number of records in the storage engine after loading

    ***************************************************************************/

    ulong testData ( ubyte[] data, DumpManager.OutOfRangeHandling
        out_of_range_handling, hash_t min, hash_t max )
    {
        auto storage = new DummyStorageEngine(min, max);
        auto input = new DummyChannelLoader(data);
        input.open();

        DumpManager.loadChannel(storage, input, out_of_range_handling);

        return storage.num_records;
    }

    ubyte[] data = [
        0,0,0,0,0,0,0,0, // version number
        16,0,0,0,0,0,0,0, // key 1 len
        48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,49, // key 1 (..001)
        4,0,0,0,0,0,0,0, // value 1 len
        1,2,3,4, // value 1
        16,0,0,0,0,0,0,0, // key 2 len
        48,48,48,48,48,48,48,48,49,48,48,48,48,48,48,48, // key 2 (..010..)
        4,0,0,0,0,0,0,0, // value 2 len
        1,2,3,4, // value 2
        16,0,0,0,0,0,0,0, // key 3 len
        49,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48, // key 3 (100..)
        4,0,0,0,0,0,0,0, // value 3 len
        1,2,3,4, // value 3
        0,0,0,0,0,0,0,0 // EOF
    ];

    // no out-of-range records
    test!("==")(testData(data, DumpManager.OutOfRangeHandling.Ignore,
        hash_t.min, hash_t.max), 3);

    // load out-of-range records
    test!("==")(testData(data, DumpManager.OutOfRangeHandling.Load, 0, 1), 3);

    // ignore out-of-range records
    test!("==")(testData(data, DumpManager.OutOfRangeHandling.Ignore, 0, 1), 0);
    test!("==")(testData(data, DumpManager.OutOfRangeHandling.Ignore,
        0x0000000100000000, hash_t.max), 2);

    // fatal upon out-of-range record
    testThrown!(Exception)(testData(data, DumpManager.OutOfRangeHandling.Fatal, 0, 1));
}



/*******************************************************************************

    Tests for invalid key handling.

*******************************************************************************/

unittest
{
    /***************************************************************************

        Calls DumpManager.loadChannel() with the provided input data.

        Params:
            data = data to load

        Returns:
            the number of records in the storage engine after loading

    ***************************************************************************/

    ulong testData ( ubyte[] data )
    {
        auto storage = new DummyStorageEngine;
        auto input = new DummyChannelLoader(data);
        input.open();

        DumpManager.loadChannel(storage, input,
            DumpManager.OutOfRangeHandling.Load);

        return storage.num_records;
    }

    ubyte[] data_header = [ 0,0,0,0,0,0,0,0 ]; // version number
    ubyte[] data_footer = [ 0,0,0,0,0,0,0,0 ]; // EOF

    // load good record
    ubyte[] good = [
        16,0,0,0,0,0,0,0, // key len
        48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,49, // good key
        4,0,0,0,0,0,0,0, // value len
        1,2,3,4 // value
    ];
    test!("==")(testData(data_header ~ good ~ data_footer), 1);

    // ignore record with short key
    ubyte[] short_key = [
        15,0,0,0,0,0,0,0, // key len
        48,48,48,48,48,48,48,48,48,48,48,48,48,48,49, // short key
        4,0,0,0,0,0,0,0, // value len
        1,2,3,4 // value
    ];
    test!("==")(testData(data_header ~ short_key ~ data_footer), 0);

    // ignore record with long key
    ubyte[] long_key = [
        17,0,0,0,0,0,0,0, // key len
        48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,49, // short key
        4,0,0,0,0,0,0,0, // value len
        1,2,3,4 // value
    ];
    test!("==")(testData(data_header ~ long_key ~ data_footer), 0);

    // ignore record with non-hexadecimal key
    ubyte[] bad_key = [
        16,0,0,0,0,0,0,0, // key len
        47,48,48,48,48,48,48,48,48,48,48,48,48,48,48,49, // bad key
        4,0,0,0,0,0,0,0, // value len
        1,2,3,4 // value
    ];
    test!("==")(testData(data_header ~ bad_key ~ data_footer), 0);
}



/*******************************************************************************

    Track the progress of a task and measures its progress.

    Used as a helper class for loading and dumping.

*******************************************************************************/

private scope class ProgressManager
{
    /***************************************************************************

        Stopwatch to use to measure the processing time.

    ***************************************************************************/

    private StopWatch sw;


    /***************************************************************************

        Name of the activity / process we are timing.

    ***************************************************************************/

    private char[] activity;


    /***************************************************************************

        Name of the particular instance of the activity / process.

    ***************************************************************************/

    private char[] name;


    /***************************************************************************

        Value to be considered 100%.

    ***************************************************************************/

    private ulong max;


    /***************************************************************************

        Current value.

    ***************************************************************************/

    private ulong curr;


    /***************************************************************************

        If true, the progress will be printed too, otherwise only the time it
        took to complete is printed at the end.

    ***************************************************************************/

    private bool verbose;


    /***************************************************************************

        Time of the previous progress display.

    ***************************************************************************/

    private float prev_time;


    /***************************************************************************

        Minimum advance (in seconds) necessary for a progress() to be printed.
        1/15 seems to be a pretty decent "framerate" for a "progress bar".

    ***************************************************************************/

   private  static const MinAdvanceSecs = 1.0 / 15.0;


    /***************************************************************************

        Constructor.

        Params:
            activity = name of the activity / process we are timing
            name = name of the particular instance of the activity / process
            max = value to be considered 100%
            verbose = if true, the progress will be printed too, otherwise only
                    the time it took to complete is printed at the end

    ***************************************************************************/

    public this ( char[] activity, char[] name, ulong max,
            bool verbose = true )
    {
        this.activity = activity;
        this.name = name;
        this.max = max;
        this.curr = 0;
        this.prev_time = 0;
        this.verbose = verbose;
        this.sw.start;

        if ( this.verbose )
        {
        }
    }


    /***************************************************************************

        When in verbose mode, reports the progress if appropriate.

        This method should be called each time there is a progress in the
        process being timed.

        Params:
            advance = number of units advanced in this progress

    ***************************************************************************/

    public void progress ( ulong advance )
    {
        auto old_value = this.curr;
        this.curr += advance;

        if ( this.verbose && this.elapsed > this.prev_time + MinAdvanceSecs)
        {
            StaticTrace.format("  {}: {}%", this.name,
                    (cast(float) this.curr / max) * 100.0f);

            this.prev_time = this.elapsed;
        }
    }


    /***************************************************************************

        Destructor. Reports the time it took to process the activity (if
        verbose).

    ***************************************************************************/

    ~this ( )
    {
        if ( this.verbose )
        {
            Stderr.format("{} '{}' in {}s",
                    this.activity, this.name, this.elapsed)
                .clearline.newline;
        }
    }


    /***************************************************************************

        Get the current value of the progress.

    ***************************************************************************/

    public final ulong current ( )
    {
        return this.curr;
    }


    /***************************************************************************

        Get the value that represents 100%.

    ***************************************************************************/

    public final ulong maximum ( )
    {
        return this.max;
    }


    /***************************************************************************

        Return the time elapsed since object construction in seconds.

    ***************************************************************************/

    public float elapsed ( )
    {
        return this.sw.microsec() / 1_000_000.f;
    }
}

