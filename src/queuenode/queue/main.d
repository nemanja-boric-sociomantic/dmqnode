/*******************************************************************************

    Queue Node Server

    copyright:  Copyright (c) 2011 sociomantic labs. All rights reserved

    version:    October 2010: Initial release
                May 2013: Combined dht and queue project

    authors:    David Eckardt, Gavin Norman
                Thomas Nicolai, Lars Kirchhoff
                Hans Bjerkander


*******************************************************************************/

module queuenode.queue.main;



/*******************************************************************************

    Imports

*******************************************************************************/

private import Version;

private import queuenode.queue.app.config.ServerConfig;
private import queuenode.queue.app.config.PerformanceConfig;
private import queuenode.queue.app.config.StatsConfig;

private import queuenode.common.util.Terminator;

private import queuenode.common.periodic.Periodics;
private import queuenode.queue.app.periodic.PeriodicQueueStats;
private import queuenode.common.periodic.PeriodicWriterFlush;

private import queuenode.queue.storage.Ring;
private import queuenode.queue.node.QueueNode;

private import swarm.queue.QueueConst;

private import ocean.core.MessageFiber;

private import ocean.io.Stdout;

private import ocean.io.select.EpollSelectDispatcher;
private import ocean.io.select.protocol.generic.ErrnoIOException : IOWarning;
private import ocean.io.select.client.SignalEvent;
private import ocean.io.select.client.model.ISelectClient;

private import ocean.util.app.LoggedCliApp;
private import ocean.util.app.ext.VersionArgsExt;

private import ConfigReader = ocean.util.config.ClassFiller;

private import tango.core.Exception : IllegalArgumentException, OutOfMemoryException;

private import tango.stdc.posix.signal: SIGINT, SIGTERM, SIGQUIT;

private import tango.util.log.Log;



/*******************************************************************************

    Setup the logger for this module

*******************************************************************************/

static Logger log;
static this ( )
{
    log = Log.lookup("queuenode.queue.main");
}



/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts queue node.

    Params:
        cl_args = array with raw command line arguments

*******************************************************************************/

private int main ( char[][] cl_args )
{
    auto app = new QueueNodeServer;
    return app.main(cl_args);
}



/*******************************************************************************

    QueueServer

*******************************************************************************/

public class QueueNodeServer : LoggedCliApp
{
    /***************************************************************************

        Version information extension.

    ***************************************************************************/

    public VersionArgsExt ver_ext;


    /***************************************************************************

        Epoll selector instance

    ***************************************************************************/

    private EpollSelectDispatcher epoll;


    /***************************************************************************

        Queue node instance

     **************************************************************************/

    private QueueNode node;


    /***************************************************************************

        SIGINT handler event

    ***************************************************************************/

    private SignalEvent sigint_event;


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


    /***************************************************************************

         Constructor

    ***************************************************************************/

    public this ( )
    {
        const app_name = "queuenode";
        const app_desc = "queuenode: distributed queue server node.";
        const usage = null;
        const help = null;
        const use_insert_appender = false;
        const loose_config_parsing = false;
        const char[][] default_configs = [ "etc/config.ini" ];

        super(app_name, app_desc, usage, help, use_insert_appender,
                loose_config_parsing, default_configs, config);

        this.ver_ext = new VersionArgsExt(Version);
        this.args_ext.registerExtension(this.ver_ext);
        this.log_ext.registerExtension(this.ver_ext);
        this.registerExtension(this.ver_ext);

        this.epoll = new EpollSelectDispatcher;
    }


    /***************************************************************************

        Get values from the configuration file.

    ***************************************************************************/

    public override void processConfig ( IApplication app, ConfigParser config )
    {
        ConfigReader.fill("Stats", this.stats_config, config);
        ConfigReader.fill("Server", this.server_config, config);
        ConfigReader.fill("Performance", this.performance_config, config);

        this.node = new QueueNode(
                QueueConst.NodeItem(this.server_config.address(),
                    this.server_config.port()),
                new RingNode(this.server_config.data_dir,
                    this.server_config.size_limit,
                    this.server_config.channel_size_limit()),
                this.epoll, this.server_config.backlog);

        this.node.error_callback = &this.nodeError;
        this.node.connection_limit = this.server_config.connection_limit;

        this.sigint_event = new SignalEvent(&this.sigintHandler,
            [SIGINT, SIGTERM, SIGQUIT]);

        this.periodics = new Periodics(this.node, this.epoll);
        this.periodics.add(new PeriodicQueueStats(this.stats_config, this.epoll));
        this.periodics.add(new PeriodicWriterFlush(
            this.epoll, this.performance_config.write_flush_ms));
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
        this.epoll.register(this.sigint_event);

        this.periodics.register();

        this.node.register(this.epoll);

        Stdout.formatln("Starting event loop");
        this.epoll.eventLoop();
        Stdout.formatln("Event loop exited");

        return true;
    }


    /***************************************************************************

        Callback for exceptions inside the node's event loop. Writes errors to
        the error.log file, and optionally to the console (if the
        Log/console_echo_errors config parameter is true).

        Params:
            exception = exception which occurred
            event_info = info about epoll event during which exception occurred

    ***************************************************************************/

    private void nodeError ( Exception exception, IAdvancedSelectClient.Event event_info )
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

    private void sigintHandler ( SignalEvent.SignalInfo siginfo )
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

