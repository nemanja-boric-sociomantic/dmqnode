/*******************************************************************************

    DHT Node Server Daemon

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        June 2009:    Initial release
                    January 2011: Asynchronous dht node
                    May 2013: Combined dht and queue project

    authors:        David Eckardt, Gavin Norman
                    Thomas Nicolai, Lars Kirchhoff
                    Hans Bjerkander

*******************************************************************************/

module swarmnodes.dht.main;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.common.kvstore.app.IKVNodeApp;

private import swarmnodes.common.util.Terminator;

private import swarmnodes.dht.app.periodic.MemoryPeriodicStats;
private import swarmnodes.dht.app.periodic.ChannelDumpThread;

private import swarmnodes.dht.storage.MemoryStorageChannels;

private import ocean.io.Stdout;

private import ocean.util.config.ClassFiller : LimitInit;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("swarmnodes.dht.main");
}



/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts dht node.

    Params:
        cl_args = array with raw command line arguments

*******************************************************************************/

private int main ( char[][] cl_args )
{
    auto app = new DhtNodeServer;
    return app.main(cl_args);
}



/*******************************************************************************

    KVNode

*******************************************************************************/

public class DhtNodeServer : IKVNodeApp
{
    /***************************************************************************

        Memory node config values

    ***************************************************************************/

    private static class MemoryConfig
    {
        ulong size_limit = 0; // 0 := no size limit
        uint dump_period = 3600; // default = 1 hour
        bool disable_dump_thread = false;
        LimitInit!(char[], "load", "load", "fatal", "ignore") allow_out_of_range;
        bool disable_direct_io = false;
        uint bnum = 0; // 0 := use tokyocabinet's default number of buckets


        /***********************************************************************

            Returns:
                OutOfRangeHandling enum value corresponding to value of
                allow_out_of_range, read from config file

        ***********************************************************************/

        public MemoryStorageChannels.OutOfRangeHandling out_of_range_handling ( )
        {
            with ( MemoryStorageChannels.OutOfRangeHandling )
            switch ( this.allow_out_of_range() )
            {
                case "load":    return Load;
                case "fatal":   return Fatal;
                case "ignore":  return Ignore;
                default:
                    assert(false);
            }
        }
    }

    private MemoryConfig memory_config;


    /***************************************************************************

        Channel dumper thread.

    ***************************************************************************/

    private ChannelDumpThread channel_dumper;


    /***************************************************************************

        Get values from the configuration file. Overridden to read additional
        memory config options.

        Params:
            app = application instance
            config = config parser instance

    ***************************************************************************/

    protected override void processConfig ( IApplication app, ConfigParser config )
    {
        super.processConfig(app, config);

        ConfigReader.fill("Options_Memory", this.memory_config, config);
    }


    /***************************************************************************

        Returns:
            a new memory storage channels instance.

    ***************************************************************************/

    override protected KVStorageChannels newStorageChannels_ ( )
    {
        return new MemoryStorageChannels(this.server_config.data_dir,
            this.memory_config.size_limit, this.hash_range,
            this.memory_config.bnum, this.memory_config.out_of_range_handling,
            this.memory_config.disable_direct_io);
    }


    /***************************************************************************

        Sets up any periodics required by the node. Calls the super class'
        method and sets up the channel dump thread and the memory dht stats
        periodic (which relies on the channel dump thread).

        Params:
            periodics = periodics instance to which periodics can be added

    ***************************************************************************/

    protected override void initPeriodics ( Periodics periodics )
    {
        super.initPeriodics(periodics);

        if ( !this.memory_config.disable_dump_thread )
        {
            this.channel_dumper = new ChannelDumpThread(
                cast(MemoryStorageChannels)this.storage_channels,
                this.memory_config.dump_period);
            this.channel_dumper.start();
        }

        periodics.add(new MemoryPeriodicStats(this.stats_config, this.epoll,
            this.channel_dumper));
    }


    /***************************************************************************

        At node shutdown, waits for the channel dump thread to finish what it's
        doing (if it is indeed doing something) before continuing with the
        storage channels shutdown.

    ***************************************************************************/

    override protected void shutdown ( )
    {
        assert(Terminator.terminating);

        if ( this.channel_dumper )
        {
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
}

