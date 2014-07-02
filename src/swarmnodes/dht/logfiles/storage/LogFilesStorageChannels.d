/*******************************************************************************

    Logfiles storage engine

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2010: Initial release
                    January 2011: Asynchronous version

    authors:        David Eckardt, Gavin Norman

*******************************************************************************/

module swarmnodes.dht.logfiles.storage.LogFilesStorageChannels;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.dht.DhtConst;

private import swarm.dht.DhtHash;

private import swarmnodes.dht.common.storage.DhtStorageChannels;

private import swarmnodes.dht.common.storage.DhtStorageEngine;

private import swarmnodes.dht.common.storage.IStepIterator;

private import swarmnodes.dht.logfiles.storage.LogRecord,
               swarmnodes.dht.logfiles.storage.LogRecordPut;

private import ocean.core.Array : concat;
private import ocean.core.Exception : enforce;

private import ocean.io.FilePath;

private import tango.io.device.File;

debug private import ocean.util.log.Trace;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("swarmnodes.dht.common.storage.LogFilesStorageChannels");
}



/*******************************************************************************

    Logfiles storage channels class

*******************************************************************************/

public class LogFilesStorageChannels : DhtStorageChannels
{
    /***************************************************************************

        Static set of supported commands for the memory node.

    ***************************************************************************/

    private alias bool[DhtConst.Command.E] SupportedCommands;

    static private SupportedCommands supported_commands;


    /***************************************************************************

        Static constructor. Initialises the list of supported commands.

    ***************************************************************************/

    static this ( )
    {
        with ( DhtConst.Command.E )
        {
            supported_commands[PutDup] = true;
            supported_commands[GetRange] = true;
            supported_commands[GetRange2] = true;
            supported_commands[GetRangeFilter] = true;
            supported_commands[GetRangeFilter2] = true;
            supported_commands[GetAll] = true;
            supported_commands[GetAll2] = true;
            supported_commands[GetAllFilter] = true;
            supported_commands[GetAllFilter2] = true;
            supported_commands[GetAllKeys] = true;
            supported_commands[GetAllKeys2] = true;
            supported_commands[GetChannels] = true;
            supported_commands[GetChannelSize] = true;
            supported_commands[GetSize] = true;
            supported_commands[GetResponsibleRange] = true;
            supported_commands[GetSupportedCommands] = true;
            supported_commands[RemoveChannel] = true;
            supported_commands[GetNumConnections] = true;
            supported_commands[GetVersion] = true;
        }

        supported_commands.rehash;
    }


    /***************************************************************************

        Default write buffer size

    ***************************************************************************/

    static public const DefaultWriteBufferSize = LogRecordPut.DefaultBufferSize;


    /***************************************************************************

        Logfiles storage engine

    ***************************************************************************/

    private class LogFiles : DhtStorageEngine
    {
        /***********************************************************************

            LogRecordPut instance, implements the write access operations.

        ***********************************************************************/

        private LogRecordPut log_record_put;


        /***********************************************************************

            Working directory.

            In the constructor the working directory is composed from the base
            directory of the storage channels and the id of this instance.

        ***********************************************************************/

        private char[] working_dir;


        /***********************************************************************

            Constructor.

            (Note that the log_record_put member is constructed with a blank
            working directory. The correct directory is set in the initialise()
            method.)

            Params:
                id = identifier string for this instance
                min_hash = minimum hash for which node is responsible
                max_hash = maximum hash for which node is responsible

        ***********************************************************************/

        public this ( char[] id, hash_t min_hash, hash_t max_hash )
        {
            this.log_record_put = new LogRecordPut("",
                this.outer.write_buffer_size);

            super(id, min_hash, max_hash);
        }


        /***********************************************************************

            Initialiser. Called from the super constructor, as well as when a
            storage engine is re-used from the pool.

        ***********************************************************************/

        override public void initialise ( char[] id )
        {
            super.initialise(id);

            this.working_dir.concat(this.outer.dir.toString, "/", super.id);
            this.createWorkingDir();

            this.log_record_put.setDir(this.working_dir);
        }


        /***********************************************************************

            Puts a record into the database, allowing duplication.

            Params:
                key   = record key
                value = record value

            Returns:
                this instance

        ***********************************************************************/

        override typeof(this) putDup ( char[] key, char[] value )
        {
            this.log_record_put.putDup(DhtHash.straightToHash(key), value);

            super.listeners.trigger(Listeners.Listener.Code.DataReady, key);

            return this;
        }


        /***********************************************************************

            Initialises a step-by-step iterator over the keys of all records in
            the database.

            Params:
                iterator = iterator to initialise

        ***********************************************************************/

        override public typeof(this) getAll ( IStepIterator iterator )
        {
            iterator.getAll();

            return this;
        }


        /***********************************************************************

            Initialises a step-by-step iterator over the keys of all records in
            the database in the specified range.

            Params:
                iterator = iterator to initialise
                min = minimum hash to iterate over
                max = maximum hash to iterate over

        ***********************************************************************/

        override public typeof(this) getRange ( IStepIterator iterator, char[] min, char[] max )
        {
            iterator.getRange(min, max);

            return this;
        }


        /***********************************************************************

            Closes database. Commits any pending writes to disk.

            (Called from IStorageChannels when removing a channel or shutting
            down the node. In the former case, the channel is clear()ed then
            close()d. In the latter case, the channel is only close()d.)

            Returns:
                this instance

        ***********************************************************************/

        public typeof(this) close ( )
        {
            log.info("Closing logfiles channel '{}'", super.id);

            this.commit();

            return this;
        }


        /***********************************************************************

            Removes all records from database.

            (Called from IStorageChannels when removing a channel.)

            Returns:
                this instance

        ***********************************************************************/

        public typeof(this) clear ( )
        {
            log.info("Clearing (deleting) logfiles channel '{}'", super.id);

            this.log_record_put.clear();

            return this;
        }


        /***************************************************************************

            Returns:
                number of records stored

        ***************************************************************************/

        public ulong num_records ( )
        {
            return this.log_record_put.numRecords;
        }


        /***************************************************************************

            Returns:
                number of records stored

        ***************************************************************************/

        public ulong num_bytes ( )
        {
            return this.log_record_put.size;
        }


        /***********************************************************************

            Commits pending data to write.

        ***********************************************************************/

        private void commit ( )
        {
            this.log_record_put.commit();
        }


        /***********************************************************************

            Creates the working folder for this storage channel, if it doesn't
            already exist.

            Note: this.createWorkingDir creates the working directory for a
            single channel, whereas super.createWorkingDir creates the main data
            directory which contains the folders for all storage channels.

        ***********************************************************************/

        private void createWorkingDir ( )
        {
            scope path = new FilePath(this.working_dir);

            if ( path.exists )
            {
                enforce(path.isFolder(), typeof (this).stringof ~ ": '" ~
                                          path.toString() ~ "' - not a directory");
            }
            else
            {
                path.createFolder();
            }
        }
    }


    /***********************************************************************

        Logfiles storage engine iterator

        Note: although this class is static, it requires access to the private
        members of LogFiles, and is thus declared in this module.

    ***********************************************************************/

    public static class LogFilesStepIterator : IStepIterator
    {
        /*******************************************************************

            Reference to storage engine, set by setStorage() method.

            Note: using this member to store a reference to the storage
            engine being iterated over, rather than this.outer, because an
            instance of this class can be constructed by one LogFiles, and
            then re-used by others.

        *******************************************************************/

        private LogFiles storage;


        /*******************************************************************

            Path of currently open bucket file.

        *******************************************************************/

        private char[] bucket_path;


        /*******************************************************************

            Header of current record. When a record is finished with, this
            value is reset to LogRecord.RecordHeader.init.

        *******************************************************************/

        private LogRecord.RecordHeader current_header;


        /*******************************************************************

            Indicates whether the header of the current record has been read
            or not.

        *******************************************************************/

        private bool read_header;


        /*******************************************************************

            Current record key. As the log file's read position is advanced
            to the start of the next record, the length of the key buffer is
            set to 0. When the key() method is called, the buffer is then
            filled with the key of the current record. The key is only
            written once into this buffer per record, and the key() method
            will then simply return the contents of the buffer.

        *******************************************************************/

        private char[] key_buffer;


        /*******************************************************************

            Current record value. As the log file's read position is
            advanced to the start of the next record, the length of the
            value buffer is set to 0. When the value() method is called, the
            buffer is then filled with the value of the current record. The
            value is only written once into this buffer per record, and the
            value() method will then simply return the contents of the
            buffer.

        *******************************************************************/

        private char[] value_buffer;


        /*******************************************************************

            Minimum and maximum keys to iterate over.

        *******************************************************************/

        private hash_t min_hash;

        private hash_t max_hash;


        /*******************************************************************

            Hash of first record in the current bucket. Used by the
            LogRecord.getNextBucket() method.

        *******************************************************************/

        private hash_t current_bucket_start;


        /*******************************************************************

            File instance.

        *******************************************************************/

        private File file;


        /*******************************************************************

            Constructor.

            Params:
                storage = storage engine to iterate over

        *******************************************************************/

        public this ( )
        {
            this.file = new File();
        }


        /*******************************************************************

            Storage initialiser.

            Params:
                storage = storage engine to iterate over

        *******************************************************************/

        public void setStorage ( DhtStorageEngine storage )
        {
            this.storage = cast(LogFiles)storage;
        }


        /*******************************************************************

            Initialises the iterator to iterate over all records in the
            storage engine. The first key is queued up, ready to be fetched
            with the methods below.

        *******************************************************************/

        public void getAll ( )
        in
        {
            assert(this.storage, typeof(this).stringof ~ ".getAll: storage not set");
        }
        body
        {
            this.reset(hash_t.min, hash_t.max);

            // Get the name of the first bucket file.
            auto no_buckets = LogRecord.getFirstBucket(this.storage.working_dir,
                    this.bucket_path, this.current_bucket_start);
            this.initFirstRecord(no_buckets);
        }


        /*******************************************************************

            Initialises the iterator to iterate over all records in the
            storage engine within the specified range of keys. The first key
            in the specified range is queued up, ready to be fetched with
            the methods below.

            Params:
                min = string containing the hexadecimal key of the first
                    record to iterate
                max = string containing the hexadecimal key of the last
                    record to iterate

        *******************************************************************/

        public void getRange ( char[] min, char[] max )
        in
        {
            assert(this.storage, typeof(this).stringof ~ ".getRange: storage not set");
        }
        body
        {
            this.reset(DhtHash.straightToHash(min), DhtHash.straightToHash(max));

            auto no_buckets = LogRecord.getFirstBucketInRange(
                this.storage.working_dir, this.bucket_path,
                this.current_bucket_start, this.min_hash, this.max_hash);
            this.initFirstRecord(no_buckets);
        }


        /*******************************************************************

            Gets the key of the current record the iterator is pointing to.

            Returns:
                current key

        *******************************************************************/

        public char[] key ( )
        {
            if ( this.read_header && !this.key_buffer.length )
            {
                this.key_buffer.length = 8;
                DhtHash.toString(this.current_header.key, this.key_buffer);
            }

            return this.key_buffer;
        }


        /*******************************************************************

            Gets the value of the current record the iterator is pointing
            to.

            Returns:
                current value

        *******************************************************************/

        public char[] value ( )
        {
            if ( this.read_header && !this.value_buffer.length )
            {
                LogRecord.readRecordValue(this.file, this.current_header, this.value_buffer);
            }

            return this.value_buffer;
        }


        /*******************************************************************

            Advances the iterator to the next record.

        *******************************************************************/

        public void next ( )
        in
        {
            assert(this.storage, typeof(this).stringof ~ ".next: storage not set");
        }
        body
        {
            bool end_of_bucket, end_of_channel;

            do
            {
                // If the last record's header was read, but the value was
                // not read, we need to seek the file's read position to
                // the start of the next record's header (this is usually
                // done when the record value is read).
                if ( this.read_header && !this.value_buffer.length )
                {
                    LogRecord.skipRecordValue(this.file, this.current_header);
                }

                this.startNextRecord();

                end_of_bucket = LogRecord.nextRecord(this.file,
                    this.current_header);

                if ( end_of_bucket )
                {
                    this.file.close();

                    hash_t next_bucket_start;
                    end_of_channel = LogRecord.getNextBucket(
                        this.storage.working_dir, this.bucket_path,
                        next_bucket_start,
                        this.current_bucket_start, this.max_hash);

                    if ( !end_of_channel )
                    {
                        this.current_bucket_start = next_bucket_start;
                        this.file.open(this.bucket_path, File.ReadExisting);
                    }
                }
                else
                {
                    this.read_header = true;
                }
            }
            while ( end_of_bucket && !end_of_channel );

            if ( end_of_channel )
            {
                this.current_header = this.current_header.init;
            }
        }


        /*******************************************************************

            Performs required de-initialisation behaviour - closes any open
            bucket file.

        *******************************************************************/

        public void finished ( )
        {
            this.reset(0, 0);
        }


        /*******************************************************************

            Resets all class members to their initial state.

            Params:
                min = minimum hash to iterate over
                max = maximum hash to iterate over

        *******************************************************************/

        private void reset ( hash_t min, hash_t max )
        in
        {
            assert(this.storage, typeof(this).stringof ~ ".getAll - storage not set");
        }
        body
        {
            this.min_hash = min;
            this.max_hash = max;

            this.bucket_path.length = 0;

            this.current_bucket_start = 0;

            this.startNextRecord();

            this.file.close;

            this.storage.commit();
        }


        /*******************************************************************

            Initialises the iterator with the first record, opening the
            bucket file if one exists.

            Params:
                no_buckets = flag indicating whether any bucket files exist
                    in the specified range

        *******************************************************************/

        private void initFirstRecord ( bool no_buckets )
        {
            this.startNextRecord();

            if ( !no_buckets )
            {
                this.file.open(this.bucket_path, File.ReadExisting);
                this.next();
            }
        }


        /*******************************************************************

            Resets all members relating to the reading of a record, ready to
            read the next record from the log file. The hash of the last
            record, if one was read, is stored.

        *******************************************************************/

        private void startNextRecord ( )
        {
            this.read_header = false;

            this.current_header = this.current_header.init;

            this.key_buffer.length = 0;
            this.value_buffer.length = 0;
        }
    }


    /***************************************************************************

        Logfile write buffer size in bytes

    ***************************************************************************/

    private const size_t write_buffer_size;


    /***************************************************************************

        Constructor. If the specified data directory exists, it is scanned for
        dumped queue channels, which are loaded. Otherwise the data directory is
        created.

        Params:
            dir = data directory for logfiles
            size_limit = maximum number of bytes allowed in the node (0 = no
                limit)
            min_hash = minimum hash for which node is responsible
            max_hash = maximum hash for which node is responsible
            write_buffer_size = size in bytes of file write buffer

    ***************************************************************************/

    public this ( char[] dir, ulong size_limit, hash_t min_hash, hash_t max_hash,
        size_t write_buffer_size = DefaultWriteBufferSize )
    {
        super(dir, size_limit, min_hash, max_hash);

        this.write_buffer_size = write_buffer_size;

        this.loadChannels();
    }


    /***************************************************************************

        Creates a new instance of an iterator for this storage engine.

        Returns:
            new iterator

    ***************************************************************************/

    public IStepIterator newIterator ( )
    {
        return new LogFilesStepIterator;
    }


    /***************************************************************************

        Tells whether a command is supported by this set of storage channels.

        Params:
            cmd = command to check

        Returns:
            true if command is supported

     **************************************************************************/

    public bool commandSupported ( DhtConst.Command.E cmd )
    {
        return (cmd in supported_commands) !is null;
    }


    /***************************************************************************

        Returns:
             string identifying the type of the storage engine

    ***************************************************************************/

    public char[] type ( )
    {
        return LogFiles.stringof;
    }


    /***************************************************************************

        Creates a new DhtStorageEngine instance of the LogFiles class with the
        specified id.

        Params:
            id = channel id

        Returns:
            new DhtStorageEngine instance

    ***************************************************************************/

    protected override DhtStorageEngine create_ ( char[] id )
    {
        return new LogFiles(id, this.min_hash, this.max_hash);
    }


    /***************************************************************************

        Searches this.dir for subdirectories and retrieves the names of the
        subdirectories as storage engine identifiers.

    ***************************************************************************/

    protected override void loadChannels ( )
    {
        log.info("Scanning {} for logfiles directories", this.dir.toString);
        debug Trace.formatln("Scanning {} for logfiles directories",
            this.dir.toString);

        foreach ( info; this.dir )
        {
            if ( info.folder )
            {
                auto id = info.name.dup;

                log.info("Opening logfiles directory '{}'", id);
                debug Trace.formatln("    Opening logfiles directory '{}'", id);

                this.create(id);
            }
            else
            {
                log.warn("Ignoring file '{}' in data directory {}",
                    info.name, this.dir.toString);
            }
        }

        log.info("Finished scanning {} for logfiles directories",
            this.dir.toString);
        debug Trace.formatln("Finished scanning {} for logfiles directories",
            this.dir.toString);
    }
}

