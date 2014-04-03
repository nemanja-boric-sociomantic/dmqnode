/*******************************************************************************

    Memory dumps manager

    copyright:      Copyright (c) 2013 sociomantic labs. All rights reserved

    authors:        Leandro Lucarella

    This module manages all the dumping/loading related operations for channels.
    It handles opening files, doing the actual dumping, renaming, backing up,
    etc.

*******************************************************************************/

module src.mod.dht.storage.memory.DumpManager;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.dht.storage.memory.DirectIO;

private import src.mod.dht.storage.model.DhtStorageEngine;

private import src.mod.dht.storage.model.IStepIterator;

private import ocean.io.FilePath;

private import ocean.io.serialize.SimpleSerializer;

private import ocean.util.log.StaticTrace;

private import tango.time.StopWatch;

private import tango.io.model.IConduit : InputStream, OutputStream;

private import tango.io.device.File;

private import ocean.io.Stdout;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("src.mod.dht.storage.memory.DumpManager");
}



/*******************************************************************************

    Dump manager class.

    This class takes care of all dumping/loading operations for the channels,
    including opening the files that channels should use to dump and load.

    It takes care of backups and deleting dumps too, and about dump files
    integrity, making sure a dump file have the right file name only when it was
    completed successfully. To handle this different file stages, it applies
    different suffixes to file, depending on in which stage is it (dumping,
    done, backup, deleted).

*******************************************************************************/

public class DumpManager
{
    /***********************************************************************

        Callback type used to create a new storage engine instance.

        Params:
            id = name of the channel than needs to be created

    ***********************************************************************/

    public alias DhtStorageEngine delegate ( char[] id ) NewChannelCb;


    /***********************************************************************

        File suffix constants

    ***********************************************************************/

    private const DumpFileSuffix = ".tcm";

    private const NewFileSuffix = ".dumping";

    private const BackupFileSuffix = ".backup";

    private const DeletedFileSuffix = ".deleted";


    /***********************************************************************

        Direct I/O files buffer size.

        See BufferedDirectWriteFile for details on why we use 32MiB.

    ***********************************************************************/

    private const IOBufferSize = 32 * 1024 * 1024;


    /***********************************************************************

        Output buffered direct I/O file, used to dump the channels.

    ***********************************************************************/

    private const BufferedDirectWriteFile output;


    /***********************************************************************

        Input buffered direct I/O file, used to load the channel dumps.

    ***********************************************************************/

    private const BufferedDirectReadFile input;


    /***********************************************************************

        File paths, re-used for various file operations

    ***********************************************************************/

    private const FilePath path;

    private const FilePath dst_path;


    /***********************************************************************

        Root directory used to look for files and write dump files.

    ***********************************************************************/

    private const FilePath root_dir;


    /***********************************************************************

        DhtStorageEngine iterator to use while dumping.

    ***********************************************************************/

    private const IStepIterator iterator;


    /***********************************************************************

       Strings used for db iteration during load of a dump file.

    ***********************************************************************/

    private char[] load_key, load_value;


    /***************************************************************************

        Constructor.

        Params:
            root_dir = root directory used to look for files and write dumps.
            iterator = DhtStorageEngine iterator instance to use for dumping.

    ***************************************************************************/

    public this ( FilePath root_dir, IStepIterator iterator )
    {
        this.root_dir = new FilePath(root_dir.toString());
        this.iterator = iterator;

        this.path = new FilePath;
        this.dst_path = new FilePath;

        auto buffer = new ubyte[IOBufferSize];
        this.output = new BufferedDirectWriteFile(null, buffer);
        this.input = new BufferedDirectReadFile(null, buffer);
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
            swapNewAndBackupDumps() for details on the rotation algorithm.

    ***************************************************************************/

    public void dump ( DhtStorageEngine storage, bool verbose = false )
    {
        this.buildFilePath(this.path, storage.id).cat(NewFileSuffix);

        if ( this.path.exists() )
        {
            log.warn("{}: OVERWRITING an old, unfinished dump file! "
                "Seems like the node wasn't properly shut down.", this.path);
            Stderr.formatln("{}: OVERWRITING an old, unfinished dump file! "
                "Seems like the node wasn't properly shut down.", this.path);
        }

        // If there are no records in the databse, then nothing to save.
        if ( storage.num_records == 0 )
        {
            log.trace("Not dumping empty channel '{}'", storage.id);
            return;
        }

        // Make the dump and close the file after leaving this scope
        {
            this.output.open(this.path.toString());
            scope (exit) this.output.close();

            this.dumpToStream(storage, this.output, verbose);
        }

        // Move dump.new -> dump and dump -> dump.backup as atomically as
        // possible
        this.swapNewAndBackupDumps(storage.id);

        log.info("Finished channel dump write and backup, {} bytes written",
            this.buildFilePath(this.path, storage.id).fileSize());
    }


    /***********************************************************************

        Writes the contents of a storage engine to a stream.

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
            storage = DhtStorageEngine to dump
            output = stream where to write the channel dump
            verbose = if true, print progrss on the dumping

    ***********************************************************************/

    private void dumpToStream ( DhtStorageEngine storage, OutputStream output,
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

        // Write the transitional version number 0 (has to be ulong)
        SimpleSerializer.write(output, 0LU);

        // Write records
        iterator.setStorage(storage);
        for ( storage.getAll(this.iterator); !this.iterator.lastKey();
                this.iterator.next() )
        {
            // TODO: handle case where out of disk space

            SimpleSerializer.write(output, this.iterator.key);
            SimpleSerializer.write(output, this.iterator.value);

            progress_manager.progress(1);
        }

        // Write the end marker (which is the empty string that just broke the
        // loop
        SimpleSerializer.write(output, this.iterator.key);

        log.info("Finished dumping channel '{}' to disk, took {}s, "
            "wrote {} records, {} records in channel",
            storage.id, progress_manager.elapsed, progress_manager.current,
            storage.num_records);
    }


    /***************************************************************************

        Load channel dump files found in this.root_dir.

        We look for dump files in the directory and load the channel when
        a valid found is found. Backup and deleted files are ignored, but other
        files trigger a warning message. Ongoing file dumps trigger
        a differentiated warning message.

        Params:
            new_channel = callback to use to create new channels

    ***************************************************************************/

    public void loadChannels ( NewChannelCb new_channel )
    {
        foreach ( info; this.root_dir )
        {
            if ( info.folder )
            {
                log.warn("Ignoring subdirectory '{}' in data directory {}",
                        path.name, this.root_dir.toString);
                Stderr.formatln("Ignoring subdirectory '{}' in data directory {}",
                        path.name, this.root_dir.toString);
                continue;
            }

            this.path.set(info.name);

            if ( this.path.suffix() == this.DumpFileSuffix )
            {
                // We don't reuse this.path for the complete path to avoid
                // conflicts between buffers
                this.buildFilePath(this.dst_path, this.path.name);

                this.input.open(this.dst_path.toString());
                scope (exit) this.input.close();

                auto channel = new_channel(this.dst_path.name.dup);
                this.loadFromStream(channel, this.input);
            }
            else if ( this.path.suffix() == this.NewFileSuffix )
            {
                log.warn("{}: Unfinished dump file found while scanning "
                        "directory '{}', the program was probably "
                        "restarted uncleanly and data might be old",
                        this.path.name, this.root_dir.toString);
                Stderr.formatln("{}: Unfinished dump file found while scanning "
                        "directory '{}', the program was probably "
                        "restarted uncleanly and data might be old",
                        this.path.name, this.root_dir.toString);
            }
            else if ( this.path.suffix() != this.BackupFileSuffix )
            {
                log.warn("{}: Ignoring file while scanning directory '{}' "
                        "(no '{}' suffix)", this.path.name,
                        this.root_dir.toString, this.DumpFileSuffix);
                Stderr.formatln("{}: Ignoring file while scanning directory "
                        "'{}' (no '{}' suffix)", this.path.name,
                        this.root_dir.toString, this.DumpFileSuffix);
            }
        }
    }


    /***********************************************************************

        Loads data from a previously dumped image from a stream.

        THIS CODE IS A TRANSITION BETWEEN AND OLDER DUMP FORMAT AND A NEW ONE.

        Please read the comment in dumpToStream() for details on the migration
        path.

        Params:
            storage = channel storage to load the dump to
            input = stream from where to read the channel dump

    ***********************************************************************/

    private void loadFromStream ( DhtStorageEngine storage, InputStream input )
    {
        log.info("Loading channel '{}' from disk", storage.id);
        Stderr.formatln("Loading channel '{}' from disk", storage.id);

        scope progress_manager = new ProgressManager("Loaded channel",
                    storage.id, (cast(File) input.conduit).length);

        // Read the file format version (new format) / number of records (old
        // format)
        ulong file_format_version;
        auto read = SimpleSerializer.read(input, file_format_version);

        progress_manager.progress(read);

        // Just to avoid confusion, will go away after the transition.
        ulong num_records = file_format_version;
        if ( num_records > 0 )
        {
            log.info("File is in old, versionless format");
        }

        // Return true if we have to keep reading
        static bool readNextKey(InputStream input, ref char[] key,
                ProgressManager progress_manager)
        {
            auto read = SimpleSerializer.read(input, key);

            progress_manager.progress(read);

            return key.length > 0;
        }

        while (readNextKey(input, this.load_key, progress_manager))
        {
            // This will go after the transition!
            if (num_records > 0 && progress_manager.current == num_records)
            {
                break;
            }

            read = SimpleSerializer.read(input, this.load_value);

            progress_manager.progress(read);

            storage.put(this.load_key, this.load_value);
        }

        // This will go after the transition!
        if (num_records > 0 && num_records != progress_manager.current)
        {
            log.error("Number of records mismatch while loading dump "
                    "file. Expected {} records, got {}",
                    num_records, progress_manager.current);
            throw new Exception("Number of records mismatch while "
                    "loading dump file.");
        }

        log.info("Finished loading channel '{}' from disk, took {}s, "
            "read {} bytes (file size including padding is {} bytes), "
            "{} records in channel", storage.id, progress_manager.elapsed,
            progress_manager.current, progress_manager.maximum,
            storage.num_records);
    }


    /***************************************************************************

        Virtually delete a channel dump file.

        What this method really does is renaming the old file and its backup
        with a new suffix to indicate they are removed. Files with this suffix
        (DeletedFileSuffix) are not loaded by loadChannels().

        Params:
            id = name of the channel to delete

    ***************************************************************************/

    public void deleteChannel ( char[] id )
    {
        this.buildFilePath(this.path, id);
        if ( this.path.exists )
        {
            this.dst_path.set(this.path).cat(DeletedFileSuffix);
            // file -> file.deleted
            this.path.rename(this.dst_path);
        }

        this.buildFilePath(this.path, id).cat(BackupFileSuffix);
        if ( this.path.exists )
        {
            this.dst_path.set(this.path).cat(DeletedFileSuffix);
            // file.backup -> file.backup.deleted
            this.path.rename(this.dst_path);
        }
    }

    /***********************************************************************

        Replace the existing dump with the new one, while moving the
        existing dump to dump.backup.

        Doing this completely atomically seems to be impossible (link(2)
        fails if the destination file exists), but since we want to avoid
        the situation where we end up with an invalid dump, we prioritize
        always having the plain dump (and updated).

        So, this is the procedure to "rotate" the dump as atomically as
        possible:

        1. Remove dump.backup
        2. Link (hard) dump to dump.backup
        3. Move dump.new to dump

        The worse case ever is loosing the dump.backup (if the application
        crashes or the server is rebooted between 1 and 2), but that's being
        backed up already every day.

        The important thing is we never, ever, under no circumstances, end
        up with a regular dump that is either incomplete or inexistent!
        (well, there are always exceptions, like hardware failure or kernel
        bugs ;)

        The downside is now we need disk space to hold 3 times the size of
        the channel instead of 2 times the size of the channel, because at
        some point we have all dump, dump.new and dump.backup all existing
        at the same time.

        Note: dump.new should always exist.

    ***********************************************************************/

    private void swapNewAndBackupDumps ( char[] id )
    {
        this.buildFilePath(this.path, id); // dump
        this.dst_path.set(this.path).cat(BackupFileSuffix); // dump.backup

        if ( this.dst_path.exists )
        {
            // 1. rm dump.backup
            this.dst_path.remove();
        }

        if ( this.path.exists )
        {
            // 2. ln dump dump.backup
            this.path.link(this.dst_path);
        }

        // 3. mv dump.new dump (new should always exist)
        this.path.cat(NewFileSuffix); // dump.new
        this.buildFilePath(this.dst_path, id); // dump
        this.path.rename(this.dst_path);
    }


    /***********************************************************************

        Formats the file name for a channel into a provided FilePath.  The name
        is built using the this.root_dir directory, the ID of the channel and
        the standard file type suffix.

        Params:
            path = FilePath object to set with the new file path
            id = Name of the channel to build the file path for

        Returns:
            The "path" object passed as parameter and properly reset.

    ***********************************************************************/

    private FilePath buildFilePath ( FilePath path, char[] id )
    {
        path.set(this.root_dir);
        path.append(id);
        path.cat(this.DumpFileSuffix);
        return path;
    }

}


/***************************************************************************

    Track the progress of a task and measures its progress.

    Used as a helper class for loading and dumping.

***************************************************************************/

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

