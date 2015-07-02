/*******************************************************************************

    Ring queue Storage engine

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        October 2010: Initial release

    authors:        David Eckardt, Gavin Norman

*******************************************************************************/

module dmqnode.storage.Ring;



/*******************************************************************************

    Imports

*******************************************************************************/

private import dmqnode.storage.model.StorageChannels;

private import dmqnode.storage.model.StorageEngine;

private import swarm.dmq.DmqConst;

private import dmqnode.storage.engine.DiskOverflow: DiskOverflow;

private import dmqnode.node.IDmqNodeInfo;

private import ocean.util.container.queue.FlexibleRingQueue;

private import ocean.util.container.mem.MemManager;

private import ocean.core.Exception: enforce;

private import tango.io.FilePath;

private import tango.io.device.File;

private import tango.io.Path : normalize, PathParser;

private import tango.sys.Environment;

debug private import tango.io.Stdout : Stderr;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dmqnode.storage.Ring");
}



/*******************************************************************************

    Ring node storage channels class, acts as a container for a set of ring
    queue channels.

*******************************************************************************/

public class RingNode : StorageChannels
{
    /***************************************************************************

        Dump file name suffix

    ***************************************************************************/

    static private const char[] DumpFileSuffix = ".rq"; // Must be char[] because of DMD bug 12634.


    /***************************************************************************

        Ring queue storage engine class.

    ***************************************************************************/

    private class Ring : StorageEngine
    {
        /***********************************************************************

            RingQueue instance

        ***********************************************************************/

        private FlexibleByteRingQueue queue;

        /***********************************************************************

            Disk overflow channel

        ***********************************************************************/

        private DiskOverflow.Channel overflow;

        /***********************************************************************

            Filename

        ***********************************************************************/

        private char[] filename;


        /***********************************************************************

            Re-used FilePath instance.

        ***********************************************************************/

        private const FilePath file_path;


        /***********************************************************************

            Constructor. Creates the ring queue. Also attempts to deserialize
            its contents from the specified path if the RingNode is being
            constructed (the loading of a dumped channel does not happen once
            the node has started up -- only at initialisation).

            Params:
                id  = queue (channel) identifier string
                dir = working directory
                queue_bytes = maximum queue size

        ***********************************************************************/

        public this ( char[] id, char[] dir, uint queue_bytes )
        in
        {
            assert(!this.outer.shutting_down, "Attempted to create channel '{}' during shutdown");
        }
        body
        {
            super(id);

            this.filename = FilePath.join(dir, id ~ this.outer.DumpFileSuffix);

            this.file_path = new FilePath(this.filename);

            this.overflow = this.outer.overflow.new Channel(id);

            this.queue = new FlexibleByteRingQueue(noScanMallocMemManager,
                queue_bytes);

            if ( this.outer.channels_scan )
            {
                this.loadDumpedChannel();
            }
        }


        /***********************************************************************

            Looks for and loads a saved dump of the channel's contents.

        ***********************************************************************/

        private void loadDumpedChannel ( )
        {
            debug Stderr.formatln("Loading from file {}", this.filename);

            if ( this.file_path.exists() )
            {
                debug Stderr.formatln("(File exists, loading)");

                scope file = new File(this.file_path.toString(), File.ReadExisting);
                scope ( exit ) file.close();

                this.queue.deserialize(file);
            }
            else
            {
                debug Stderr.formatln("(File doesn't exist)");
            }
        }


        /***********************************************************************

            Pushes a record into queue.

            Params:
                value = record value

        ***********************************************************************/

        override protected void push_ ( char[] value )
        {
            this.outer.dmqnode.handledRecord();

            if (!this.queue.push(cast(ubyte[])value))
            {
                this.overflow.push(value);
            }
        }


        /***********************************************************************

            Pops a record from queue.

            Params:
                value = record value

            Returns:
                this instance

        ***********************************************************************/

        public typeof(this) pop ( ref char[] value )
        {
            void[] allocValue ( size_t n )
            {
                value.length = n;
                return value;
            }

            if (void[] item = this.queue.pop())
            {
                 allocValue(item.length)[] = item[];
                 this.outer.dmqnode.handledRecord();
            }
            else if (this.overflow.pop(&allocValue))
            {
                this.outer.dmqnode.handledRecord();
            }
            else
            {
                value.length = 0;
            }

            return this;
        }


        /***********************************************************************

            Removes all records from the queue.

            Returns:
                this instance

        ***********************************************************************/

        public typeof(this) clear ( )
        {
            this.queue.clear;
            this.overflow.clear;

            return this;
        }


        /***********************************************************************

            Closes the queue.

            Returns:
                this instance

        ***********************************************************************/

        public typeof(this) close ( )
        {
            if ( this.file_path.exists )
            {
                log.warn("Closing channel '{}' -- will {} existing dump file '{}'",
                         this.id,
                         this.queue.length? "overwrite" : "delete",
                         this.file_path.toString());
            }
            else
            {
                if ( this.queue.length )
                {
                    log.info("Closing channel '{}' -- saving in file '{}'",
                             this.id, this.file_path.toString());
                }
                else
                {
                    log.info("Closing channel '{}' -- channel is empty, not saving", this.id);
                }
            }

            if ( this.queue.length )
            {
                scope file = new File(this.file_path.toString(), File.WriteCreate);
                scope ( exit ) file.close();

                this.queue.serialize(file);
            }

            return this;
        }

        /***********************************************************************

            Deletes the channel dump file.

        ***********************************************************************/

        public void deleteDumpFile ( )
        {
            this.file_path.remove();
        }

        /***********************************************************************

            Returns:
                number of records stored

        ***********************************************************************/

        public ulong num_records ( )
        {
            return this.queue.length + this.overflow.num_records;
        }


        /***********************************************************************

            Returns:
                number of records stored

        ***********************************************************************/

        public ulong num_bytes ( )
        {
            return this.queue.used_space + this.overflow.num_bytes;
        }

        /***********************************************************************

           Tells the queue capacity in bytes.

           Returns:
               the queue capacity in bytes.

        ***********************************************************************/

        public ulong capacity_bytes ( )
        {
            return queue.total_space;
        }
    }

    /***************************************************************************

        Disk overflow.

    ***************************************************************************/

    private DiskOverflow overflow;


    /***************************************************************************

        Data directory where dump files are stored.

    ***************************************************************************/

    private char[] data_dir;


    /***************************************************************************

        Delegate which is called when a record is pushed or popped. Note that a
        failed push (e.g. queue full) will call this delegate, whereas a failed
        pop (e.g. queue empty) will not.

    ***************************************************************************/

    private const IDmqNodeInfo dmqnode;


    /***************************************************************************

        Flag indicating whether a scan for dumped ring files is being performed
        (at node startup). The flags is checked by the Ring constructor to make
        sure that dumped files are only loaded during node startup, and not when
        a new channel is created when the node is running.

    ***************************************************************************/

    private bool channels_scan;


    /***************************************************************************

        Flag indicating whether the node is currently shut down to prevent
        creating a new channel if a Produce request sends a record during
        shutdown.

    ***************************************************************************/

    private bool shutting_down = false;


    /***************************************************************************

        Constructor. If the specified data directory exists, it is scanned for
        dumped queue channels, which are loaded. Otherwise the data directory is
        created.

        Params:
            data_dir = data directory for dumped queue channels
            handled_record = delegate to call upon pushing or popping
            size_limit = maximum number of bytes allowed in the node (0 = no
                limit)
            channel_size_limit = maximum number of bytes allowed per channel (0
                = no limit)

    ***************************************************************************/

    public this ( char[] data_dir, IDmqNodeInfo dmqnode,
        ulong size_limit = 0, ulong channel_size_limit = 0 )
    in
    {
        assert(dmqnode);
    }
    body
    {
        super(size_limit, channel_size_limit);

        this.dmqnode = dmqnode;

        this.data_dir = data_dir;

        scope path = new FilePath;
        this.setWorkingPath(path, this.data_dir);

        this.overflow = new DiskOverflow(data_dir);

        if ( path.exists() )
        {
            this.loadDumpedChannels(path);
        }
        else
        {
            this.createWorkingDir(path);
        }
    }

    /***************************************************************************

        Writes disk overflow index.

    ***************************************************************************/

    override public void writeDiskOverflowIndex ( )
    {
        this.overflow.writeIndex();
    }

    /***************************************************************************

        Creates a new storage engine with the given name.

        Params:
            id = identifier string for new storage engine

        Returns:
            new storage engine

        Throws:
            Exception if the node is shutting down.

    ***************************************************************************/

    protected StorageEngine create_ ( char[] id )
    {
        enforce(!this.shutting_down, "Cannot create channel '" ~ id ~
                                     "' while shutting down");
        return new Ring(id, this.data_dir, super.channelSizeLimit);
    }


    /***************************************************************************

        Calculates the size (in bytes) an item would take if it were pushed
        to the queue.

        Params:
            len = length of data item

        Returns:
            bytes that data will claim in the queue

    ***************************************************************************/

    protected size_t pushSize ( size_t additional_size )
    {
        return FlexibleByteRingQueue.pushSize(additional_size);
    }


    /***************************************************************************

        Returns:
             string identifying the type of the storage engine

    ***************************************************************************/

    public char[] type ( )
    {
        return Ring.stringof;
    }


    /***************************************************************************

        Shuts the disk overflow down and prevents creating a new channel from
        this point.

    ***************************************************************************/

    protected override void shutdown_ ( )
    {
        this.shutting_down = true;
        this.overflow.close();
    }


    /***************************************************************************

        Generates a absolute, normalized path string from path.

        Params:
            path = file path

        Returns:
            absolute, normalized path string

     **************************************************************************/

    private char[] getFullPathString ( FilePath path )
    {
        return path.set(normalize(path.folder)).toString;
    }


    /***************************************************************************

        Creates a FilePath instance set to the absolute path of dir, if dir is
        not null, or to the current working directory of the environment
        otherwise.

        Params:
            path = FilePath instance to set
            dir = directory string; null indicates that the current working
                  directory of the environment should be used

    ***************************************************************************/

    private void setWorkingPath ( FilePath path, char[] dir )
    {
        if ( dir )
        {
            path.set(dir);

            if ( !path.isAbsolute() )
            {
                path.prepend(Environment.cwd());
            }
        }
        else
        {
            path.set(Environment.cwd());
        }
    }

    /***************************************************************************

        Searches dir for files with DumpFileSuffix suffix and retrieves the file
        names without suffices as storage engine identifiers.

        Params:
            dir = directory to search for database file objects

    ***************************************************************************/

    private void loadDumpedChannels ( FilePath path )
    {
        scope filename = new FilePath;

        debug Stderr.formatln("Scanning {} for queue files", path.toString);

        {
            this.channels_scan = true;
            scope ( exit ) this.channels_scan = false;

            foreach ( info; path )
            {
                if ( !info.folder )
                {
                    switch (filename.set(info.name).suffix())
                    {
                        case DumpFileSuffix:
                            auto id = filename.name.dup;
                            debug Stderr.formatln("    Loading queue file '{}'", id);
                            this.create(id);
                            break;

                        case this.overflow.Const.datafile_suffix,
                             this.overflow.Const.indexfile_suffix:
                            break;

                        default:
                            log.warn(typeof(this).stringof ~ ": ignoring file '" ~
                                info.name ~ "' in data directory '" ~
                                this.getFullPathString(path) ~ "' (unknown suffix)");
                    }
                }
                else
                {
                    log.warn(typeof(this).stringof ~ ": found "
                        "subdirectory '" ~ info.name ~ "' in data "
                        "directory '" ~ this.getFullPathString(path) ~ '\'');
                }

            }
        }

        // Delete the dump files after successful deserialisation.

        foreach (channel_; this)
        {
            auto channel = cast(Ring)channel_;
            assert(channel);
            channel.deleteDumpFile();
        }
    }

    /***************************************************************************

        Creates data directory.

        Params:
            dir = directory to initialize; set to null to use the
                current working directory

    ***************************************************************************/

    private void createWorkingDir ( FilePath path )
    {
        try
        {
            path.createFolder();
        }
        catch (Exception e)
        {
            e.msg = typeof(this).stringof ~ ": Failed creating directory: " ~ e.msg;

            throw e;
        }
    }
}

