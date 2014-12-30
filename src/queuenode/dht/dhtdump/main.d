/*******************************************************************************

    Copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

    Dht node channel dump tool.

*******************************************************************************/

module queuenode.dht.dhtdump.main;



/*******************************************************************************

    Imports

*******************************************************************************/

private import Version;

private import queuenode.dht.dhtdump.DumpCycle;
private import queuenode.dht.dhtdump.DumpStats;

private import ocean.io.select.EpollSelectDispatcher;

private import ocean.io.select.client.TimerEvent;

private import ocean.util.app.VersionedLoggedCliApp;

private import ConfigReader = ocean.util.config.ClassFiller;

private import swarm.dht.DhtClient;

private import swarm.dht.client.helper.RetryHandshake;

private import tango.math.random.Random;

private import tango.time.StopWatch;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;

static this ( )
{
    log = Log.lookup("queuenode.dht.dhtdump.main");
}



/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts dhtdump.

    Params:
        cl_args = array with raw command line arguments

*******************************************************************************/

private int main ( char[][] cl_args )
{
    auto app = new DhtDump;
    return app.main(cl_args);
}



public class DhtDump : VersionedLoggedCliApp
{
    /***************************************************************************

        Epoll selector instance

    ***************************************************************************/

    private const EpollSelectDispatcher epoll;


    /***************************************************************************

        Dht client instance

    ***************************************************************************/

    private const DumpCycle.ScopeDhtClient dht;


    /***************************************************************************

        Dump cycle instance.

    ***************************************************************************/

    private const DumpCycle dump_cycle;


    /***************************************************************************

        Dht settings, read from config file

    ***************************************************************************/

    private static class DhtConfig
    {
        char[] address;
        ushort port;
    }

    private DhtConfig dht_config;


    /***************************************************************************

        Dump settings, read from config file

    ***************************************************************************/

    private DumpCycle.Config dump_config;


    /***************************************************************************

        Stats log settings, read from config file

    ***************************************************************************/

    private DumpStats.Config stats_config;


    /***************************************************************************

        Constructor.

    ***************************************************************************/

    public this ( )
    {
        const app_name = "dhtdump";
        const app_desc = "iterates over all channels in a dht node, dumping the"
            " data to disk";
        super(app_name, app_desc, Version);

        this.epoll = new EpollSelectDispatcher;

        this.dht = new DumpCycle.ScopeDhtClient(this.epoll,
            new DhtClient.ScopeRequestsPlugin);

        this.dump_cycle = new DumpCycle(this.epoll, this.dht);
    }


    /***************************************************************************

        Set up the arguments parser for the app.

        Params:
            app = application instance
            argument = arguments parser to initialise

    ***************************************************************************/

    public override void setupArgs ( IApplication app, Arguments args )
    {
        args("oneshot").aliased('o').
            help("one-shot mode, perform a single dump immediately then exit");
    }


    /***************************************************************************

        Do the actual application work. Called by the super class.

        Params:
            args = command line arguments
            config = parser instance with the parsed configuration

        Returns:
            status code to return to the OS

    ***************************************************************************/

    protected override int run ( Arguments args, ConfigParser config )
    {
        ConfigReader.fill("Dht", this.dht_config, config);
        ConfigReader.fill("Dump", this.dump_config, config);
        ConfigReader.fill("Stats", this.stats_config, config);

        this.initDht();

        if ( args.exists("oneshot") )
        {
            scope stats = new DumpStats;
            this.dump_cycle.one_shot = true;
            this.dump_cycle.start(this.dump_config, stats);
            this.epoll.eventLoop();
        }
        else
        {
            scope stats = new DumpStats(this.stats_config, this.epoll);
            this.dump_cycle.start(this.dump_config, stats);
            this.epoll.eventLoop();
        }

        return true;
    }


    /***************************************************************************

        Sets up the dht client for use, adding the config-specified node to the
        registry and performing the handshake.

    ***************************************************************************/

    private void initDht ( )
    {
        this.dht.addNode(this.dht_config.address, this.dht_config.port);

        class Handshake : RetryHandshake
        {
            private const retry_wait_s = 2;

            public this ( )
            {
                super(this.outer.epoll, this.outer.dht, retry_wait_s);
            }

            override protected void error ( )
            {
                log.error("Error during dht handshake, retrying in {}s",
                    this.retry_wait_s);
            }

            override protected void success ( )
            {
                log.trace("Connected to dht");
            }
        }

        new Handshake;

        this.epoll.eventLoop();
    }
}

