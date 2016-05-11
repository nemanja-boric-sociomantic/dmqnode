/*******************************************************************************

    Distributed Message Queue Node Server

    copyright:  Copyright (c) 2011 sociomantic labs. All rights reserved

    version:    October 2010: Initial release
                May 2013: Combined dht and dmq project

    authors:    David Eckardt, Gavin Norman
                Thomas Nicolai, Lars Kirchhoff
                Hans Bjerkander


*******************************************************************************/

module dmqnode.main;



/*******************************************************************************

    Imports

*******************************************************************************/

private import Version;

private import dmqnode.app.config.ServerConfig;
private import dmqnode.app.config.PerformanceConfig;
private import dmqnode.app.config.StatsConfig;
private import dmqnode.app.config.OverflowConfig;

private import dmqnode.app.util.Terminator;

private import dmqnode.app.periodic.Periodics;
private import dmqnode.app.periodic.PeriodicStats;
private import dmqnode.app.periodic.PeriodicWriterFlush;
private import dmqnode.app.periodic.PeriodicDiskOverflowIndexWriter;

private import dmqnode.storage.Ring;
private import dmqnode.node.DmqNode;

private import swarm.dmq.DmqConst;
private import swarm.core.node.model.ISwarmConnectionHandlerInfo;

private import ocean.core.MessageFiber;

private import ocean.io.Stdout;

private import ocean.io.select.EpollSelectDispatcher;
private import ocean.io.select.protocol.generic.ErrnoIOException : IOWarning;
private import ocean.io.select.client.model.ISelectClient;

private import ocean.util.app.DaemonApp;

private import ConfigReader = ocean.util.config.ClassFiller;

private import ocean.core.Exception_tango : IllegalArgumentException, OutOfMemoryException;

private import ocean.stdc.posix.signal: SIGINT, SIGTERM, SIGQUIT;

private import ocean.util.log.Log;



/*******************************************************************************

    Setup the logger for this module

*******************************************************************************/

static Logger log;
static this ( )
{
    log = Log.lookup("dmqnode.main");
}



/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts the DMQ node.

    Params:
        cl_args = array with raw command line arguments

*******************************************************************************/

private int main ( char[][] cl_args )
{
    auto app = new DmqNodeServer;
    return app.main(cl_args);
}



/*******************************************************************************

    DMQ Node Server

*******************************************************************************/

public class DmqNodeServer : DaemonApp
{
    /***************************************************************************

        Epoll selector instance

    ***************************************************************************/

    private EpollSelectDispatcher epoll;


    /***************************************************************************

        DMQ node instance

     **************************************************************************/

    private DmqNode node;


    /***************************************************************************

        Periodic processes manager

    ***************************************************************************/

    private Periodics periodics;


    /***************************************************************************

        Instances of each config class to be read.

    ***************************************************************************/

    private ServerConfig server_config;
    private PerformanceConfig performance_config;
    private StatsConfig stats_config;
    private OverflowConfig overflow_config;


    /***************************************************************************

         Constructor

    ***************************************************************************/

    public this ( )
    {
        const app_name = "dmqnode";
        const app_desc = "dmqnode: distributed message queue server node.";

        DaemonApp.OptionalSettings settings;
        settings.signals = [SIGINT, SIGTERM, SIGQUIT];

        super(this.epoll = new EpollSelectDispatcher,
              app_name, app_desc, versionInfo, settings);
    }


    /***************************************************************************

        Get values from the configuration file.

    ***************************************************************************/

    public override void processConfig ( IApplication app, ConfigParser config )
    {
        ConfigReader.fill("Stats", this.stats_config, config);
        ConfigReader.fill("Server", this.server_config, config);
        ConfigReader.fill("Performance", this.performance_config, config);
        ConfigReader.fill("Overflow", this.overflow_config, config);

        this.node = new DmqNode(this.server_config, this.epoll);

        this.node.error_callback = &this.nodeError;
        this.node.connection_limit = this.server_config.connection_limit;

        this.periodics = new Periodics(this.node, this.epoll);
        this.periodics.add!(PeriodicStats)(this.stats_config);
        this.periodics.add!(PeriodicWriterFlush)(this.performance_config.write_flush_ms);
        this.periodics.add!(PeriodicDiskOverflowIndexWriter)(this.overflow_config.write_index_ms);
    }


    /***************************************************************************

        Do the actual application work. Called by the super class.

        Params:
            args = command line arguments
            config = parser instance with the parsed configuration

        Returns:
            status code to return to the OS

    ***************************************************************************/

    protected int run ( Arguments args, ConfigParser config )
    {
        this.startEventHandling();

        this.periodics.register();

        this.node.register(this.epoll);

        Stdout.formatln("Starting event loop");
        this.epoll.eventLoop();
        Stdout.formatln("Event loop exited");

        return 0;
    }


    /***************************************************************************

        Callback for exceptions inside the node's event loop. Writes errors to
        the error.log file, and optionally to the console (if the
        Log/console_echo_errors config parameter is true).

        Params:
            exception = exception which occurred
            event_info = info about epoll event during which exception occurred

    ***************************************************************************/

    private void nodeError ( Exception exception, IAdvancedSelectClient.Event event_info,
        ISwarmConnectionHandlerInfo.IConnectionHandlerInfo conn )
    {
        if ( cast(MessageFiber.KilledException)exception ||
             cast(IOWarning)exception )
        {
            // Don't log these exception types, which only occur on the normal
            // disconnection of a client.
        }
        else if ( cast(OutOfMemoryException)exception )
        {
            log.error("OutOfMemoryException caught in eventLoop");
        }
        else
        {
            log.error("Exception caught in eventLoop: '{}' @ {}:{}",
                    exception.msg, exception.file, exception.line);
        }
    }


    /***************************************************************************

        SIGINT handler.

        Firstly unregisters all periodics. (Any periodics which are about to
        fire in epoll will still fire, but the setting of the 'terminating' flag
        will stop them from doing anything.)

        Secondly calls the node's shutdown method. This unregisters the select
        listener (stopping any more requests from being processed), then shuts
        down the storage channels.

        Finally shuts down epoll. This will result in the run() method, above,
        returning.

        Params:
            siginfo = info struct about signal which fired

    ***************************************************************************/

    override public void onSignal ( int signal )
    {
        // Due to this delegate being called from epoll, we know that none of
        // the periodics are currently active. (The dump periodic may have
        // caused the memory storage channels to fork, however.)
        // Setting the terminating flag to true prevents any periodics which
        // fire from now on from doing anything (see IPeriodics).
        Terminator.terminating = true;

        this.periodics.shutdown();

        this.node.shutdown;

        this.epoll.shutdown;
    }
}

