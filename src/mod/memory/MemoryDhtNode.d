/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        04/06/2012: Initial release

    authors:        Gavin Norman

    Memory dht node

*******************************************************************************/

module src.mod.memory.MemoryDhtNode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.core.model.IDhtNode;

private import src.core.util.Terminator;

private import src.mod.memory.MemoryPeriodicStats;

private import src.mod.memory.ChannelDumpThread;

private import swarm.dht.node.storage.MemoryStorageChannels;

private import ConfigReader = ocean.util.config.ClassFiller;

private import ocean.io.Stdout;

private import tango.util.log.Log;




/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("src.mod.node.memory.MemoryDhtNode");
}



/*******************************************************************************

    Memory node config values

*******************************************************************************/

private class MemoryConfig
{
    ulong size_limit = 0; // 0 := no size limit
    uint dump_period = 3600; // default = 1 hour
    uint bnum = 0; // TODO: what's the default?
}



/*******************************************************************************

    Memory node class

*******************************************************************************/

public class MemoryDhtNode : IDhtNode
{
    /***************************************************************************

        Memory node specific config values.

    ***************************************************************************/

    private MemoryConfig memory_config;


    /***************************************************************************

        Channel dumper thread.

    ***************************************************************************/

    private ChannelDumpThread channel_dumper;


    /***************************************************************************

        Constructor.

        Params:
            server_config = parsed server config instance
            config = config parser (used to parse memory node config values)

    ***************************************************************************/

    public this ( ServerConfig server_config, ConfigParser config )
    {
        ConfigReader.fill("Options_Memory", this.memory_config, config);

        super(server_config, config);

        this.channel_dumper = new ChannelDumpThread(
            cast(MemoryStorageChannels)this.storage_channels,
            this.memory_config.dump_period);
        this.channel_dumper.start();

        this.periodics.add(new MemoryPeriodicStats(this.stats_config,
            this.channel_dumper));
    }


    /***************************************************************************

        Returns:
            a new memory storage channels instance.

    ***************************************************************************/

    protected DhtStorageChannels newStorageChannels_ ( )
    {
        MemoryStorageChannels.Args args;
        args.bnum = this.memory_config.bnum;

        return new MemoryStorageChannels(this.server_config.data_dir,
            this.memory_config.size_limit, args);
    }


    /***************************************************************************

        At node shutdown, waits for the channel dump thread to finish what it's
        doing (if it is indeed doing something) before continuing with the
        storage channels shutdown.

    ***************************************************************************/

    override protected void shutdown ( )
    {
        assert(Terminator.terminating);

        auto dumping = this.channel_dumper.busy;
        if ( dumping )
        {
            Stdout.format("Waiting for channel dump thread to exit...").flush;
            log.info("SIGINT handler: waiting for channel dump thread to exit");
        }

        // Wait for dump thread to exit.
        this.channel_dumper.join();

        if ( dumping )
        {
            Stdout.formatln(" DONE");
            log.info("SIGINT handler: waiting for channel dump thread to exit finished");
        }
    }
}

