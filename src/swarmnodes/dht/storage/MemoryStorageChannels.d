/*******************************************************************************

    In-memory hashtable storage engine

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        June 2010       initial release
                    August 2010     revised version (32bit mem limit)

    authors:        Lars Kirchhoff, Thomas Nicolai, David Eckardt, Gavin Norman

*******************************************************************************/

module swarmnodes.dht.storage.MemoryStorageChannels;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.dht.DhtConst;

private import swarmnodes.common.kvstore.storage.KVStorageChannels;

private import swarmnodes.common.kvstore.storage.KVStorageEngine;

private import swarmnodes.common.kvstore.storage.IStepIterator;

private import swarmnodes.dht.storage.DumpManager;

private import swarmnodes.dht.storage.MemoryStorage;

debug private import ocean.io.Stdout;

private import ocean.io.FilePath;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("swarmnodes.common.kvstore.storage.MemoryStorageChannels");
}



/*******************************************************************************

    Memory storage channels class

*******************************************************************************/

public class MemoryStorageChannels : KVStorageChannels
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
            supported_commands[Put] = true;
            supported_commands[Get] = true;
            supported_commands[Exists] = true;
            supported_commands[Remove] = true;
            supported_commands[GetAll] = true;
            supported_commands[GetAllFilter] = true;
            supported_commands[GetAllKeys] = true;
            supported_commands[GetChannels] = true;
            supported_commands[GetChannelSize] = true;
            supported_commands[GetSize] = true;
            supported_commands[GetResponsibleRange] = true;
            supported_commands[GetSupportedCommands] = true;
            supported_commands[RemoveChannel] = true;
            supported_commands[GetNumConnections] = true;
            supported_commands[GetVersion] = true;
            supported_commands[Listen] = true;
            supported_commands[Redistribute] = true;
        }

        supported_commands.rehash;
    }


    /***************************************************************************

        Public alias of enum type.

    ***************************************************************************/

    public alias DumpManager.OutOfRangeHandling OutOfRangeHandling;


    /***************************************************************************

        Estimated number of buckets in map -- passed to tokyocabinet when
        creating database instances.

    ***************************************************************************/

    private const uint bnum;


    /***************************************************************************

        State of storage channels.

        The ChannelsScan state is set during the loadChannels() method, and is
        checked by the MemoryStorage constructor to make sure that dumped files
        are only loaded during node startup, and not when a new channel is
        created when the node is running.

        The ShuttingDown state is set by the shutdown_ method, and is checked by
        the dump() method of MemoryStorage.

    ***************************************************************************/

    private enum State
    {
        Init,           // Invalid
        ChannelsScan,   // Scanning for / loading dumped channels
        Running,        // Normal running state
        ShuttingDown    // Shutting down / dumping channels
    }

    private State state;


    /***************************************************************************

        Memory storage dump file manager.

    ***************************************************************************/

    private const DumpManager dump_manager;


    /***************************************************************************

        Constructor. If the specified data directory exists, it is scanned for
        dumped memory channels, which are loaded. Otherwise the data directory
        is created.

        Params:
            dir = data directory for dumped memory channels
            size_limit = maximum number of bytes allowed in the node (0 = no
                limit)
            hash_range = hash range for which this node is responsible
            bnum = estimated number of buckets in map (passed to tokyocabinet
                "ctor")
            out_of_range_handling = determines how out-of-range records (i.e.
                those whose keys are not in the range of hashes supported by the
                node) are handled (see DumpManager)
            disable_direct_io = determines if regular buffered I/O (true) or
                direct I/O is used (false). Regular I/O is only useful for
                testing, because direct I/O imposes some restrictions over the
                type of filesystem that can be used.

    ***************************************************************************/

    public this ( char[] dir, ulong size_limit, KVHashRange hash_range,
        uint bnum, OutOfRangeHandling out_of_range_handling,
        bool disable_direct_io )
    {
        super(dir, size_limit, hash_range);

        this.bnum = bnum;

        this.dump_manager = new DumpManager(this.dir, this.newIterator(),
            out_of_range_handling, disable_direct_io);

        this.loadChannels();
    }


    /***************************************************************************

        Creates a new instance of an iterator for this storage engine.

        Returns:
            new iterator

    ***************************************************************************/

    public IStepIterator newIterator ( )
    {
        return new MemoryStorageStepIterator;
    }


    /***************************************************************************

        Tells whether a command is supported by this set of storage channels.

        Params:
            cmd = command to check

        Returns:
            true if command is supported

    ***************************************************************************/

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
        return "Memory";
    }

    /***************************************************************************

        Initiates a maintenance cycle for the storage channels. This method may
        be called periodically by the node in order to perform any cleanup or
        maintenance tasks required by the storage channels.

        The MemoryStorage implementation writes the in-memory channels to
        disk.

    ***************************************************************************/

    public void maintenance ( )
    {
        if ( this.state == State.Running )
        {
            log.info("Dumping memory channels");

            synchronized ( this ) foreach ( channel; this )
            {
                this.dump_manager.dump(channel, false); // silent
            }
        }
    }


    /***************************************************************************

        Removes channel with identifier string channel_id from the registered
        channels. All records in the channel are deleted.

        Note: this method is overridden in order to synchronize the removal of a
        channel with the dumping of channel data to disk by maintenance().

        FIXME: This thread synchronisation is purely a temporary measure, as we
        intend to entirely remove threading from the dht node.
        See https://github.com/sociomantic/swarmnodes/issues/41 for discussion.

        Params:
            channel_id = identifier string of channel to remove

    ***************************************************************************/

    public override void remove ( char[] channel_id )
    {
        synchronized ( this )  super.remove(channel_id);
    }


    /***************************************************************************

        Returns:
             string identifying the type of the storage engine

    ***************************************************************************/

    protected override void shutdown_ ( )
    {
        this.state = State.ShuttingDown;

        log.info("Closing memory channels");
        debug Stdout.formatln("{}: closing memory channels", typeof(this).stringof);

        foreach ( channel; this )
        {
            this.dump_manager.dump(channel, true); // verbose
        }

        log.info("Finished closing memory channels");
        debug Stdout.formatln("{}: closing memory channels finished",
            typeof(this).stringof);
    }


    /***************************************************************************

        Creates a new KVStorageEngine instance of the MemoryStorage
        class with the specified id.

        Params:
            id = channel id

        Returns:
            new KVStorageEngine instance

    ***************************************************************************/

    protected override KVStorageEngine create_ ( char[] id )
    {
        return new MemoryStorage(id, this.hash_range, this.bnum,
                &this.dump_manager.deleteChannel);
    }


    /***************************************************************************

        Searches this.dir for files with DumpFileSuffix suffix and creates and
        load the channels with the contents of the dump files.

    ***************************************************************************/

    protected override void loadChannels ( )
    {
        log.info("Scanning {} for memory files", this.dir.toString);
        debug Stdout.formatln("Scanning {} for memory files", this.dir.toString);

        this.state = State.ChannelsScan;
        scope ( exit ) this.state = State.Running;

        this.dump_manager.loadChannels(&this.create);

        log.info("Finished scanning {} for memory files", this.dir.toString);
        debug Stdout.formatln("Finished scanning {} for memory files",
            this.dir.toString);
    }
}

