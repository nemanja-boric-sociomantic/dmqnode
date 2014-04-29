/*******************************************************************************

    In-memory hashtable storage engine

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        June 2010       initial release
                    August 2010     revised version (32bit mem limit)

    authors:        Lars Kirchhoff, Thomas Nicolai, David Eckardt, Gavin Norman

*******************************************************************************/

module src.mod.dht.storage.MemoryStorageChannels;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.dht.DhtConst;

private import src.mod.dht.storage.model.DhtStorageChannels;

private import src.mod.dht.storage.model.DhtStorageEngine;

private import src.mod.dht.storage.model.IStepIterator;

private import src.mod.dht.storage.memory.DumpManager;

private import src.mod.dht.storage.memory.MemoryStorage;

debug private import ocean.io.Stdout;

private import ocean.io.FilePath;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("src.mod.dht.storage.MemoryStorageChannels");
}



/*******************************************************************************

    Memory storage channels class

*******************************************************************************/

public class MemoryStorageChannels : DhtStorageChannels
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
            supported_commands[Listen] = true;
        }

        supported_commands.rehash;
    }


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
            min_hash = minimum hash for which node is responsible
            max_hash = maximum hash for which node is responsible
            bnum = estimated number of buckets in map (passed to tokyocabinet
                "ctor")
            allow_out_of_range = determines whether out-of-range records (i.e.
                those whose keys are not in the range of hashes supported by the
                node) are loaded (true) or rejected (false)

    ***************************************************************************/

    public this ( char[] dir, ulong size_limit, hash_t min_hash, hash_t max_hash,
        uint bnum, bool allow_out_of_range )
    {
        super(dir, size_limit, min_hash, max_hash);

        this.bnum = bnum;

        this.dump_manager = new DumpManager(this.dir, this.newIterator(),
            allow_out_of_range);

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

        Creates a new DhtStorageEngine instance of the MemoryStorage
        class with the specified id.

        Params:
            id = channel id

        Returns:
            new DhtStorageEngine instance

    ***************************************************************************/

    protected override DhtStorageEngine create_ ( char[] id )
    {
        return new MemoryStorage(id, this.min_hash, this.max_hash, this.bnum,
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

