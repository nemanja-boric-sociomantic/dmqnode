/*******************************************************************************

    Queue Node Server

    copyright:  Copyright (c) 2011 sociomantic labs. All rights reserved

    version:    October 2010: Initial release

    authors:    David Eckardt, Gavin Norman
                Thomas Nicolai, Lars Kirchhoff

    TODO: this module is extremely similar to the equivalent in the DhtNode
    project. Find a central place to combine them.

*******************************************************************************/

module src.mod.node.QueueNode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.main.Version;

private import src.mod.node.config.MainConfig;

private import src.mod.node.util.Terminator;

private import src.mod.node.periodic.Periodics;
private import src.mod.node.periodic.PeriodicStats;

private import swarm.queue.QueueNode;
private import swarm.queue.QueueConst;
private import swarm.queue.node.storage.Ring;

private import ocean.core.Exception : assertEx;
private import ocean.core.MessageFiber;

private import ocean.io.select.EpollSelectDispatcher;
private import ocean.io.select.protocol.generic.ErrnoIOException : IOWarning;
private import ocean.io.select.event.SignalEvent;
private import ocean.io.select.model.ISelectClient;

private import ocean.util.app.LoggedCliApp;
private import ocean.util.app.ext.VersionArgsExt;

private import ocean.util.OceanException; // TODO: remove

debug private import ocean.util.log.Trace;

private import tango.core.Exception : IllegalArgumentException, OutOfMemoryException;

private import tango.stdc.posix.signal: SIGINT;



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

         Constructor

     **************************************************************************/

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

    public override void processConfig ( Application app, ConfigParser config )
    {
        MainConfig.init(config);

        this.node = new QueueNode(
                QueueConst.NodeItem(MainConfig.server.address(),
                    MainConfig.server.port()),
                new RingNode(MainConfig.server.data_dir, MainConfig.server.size_limit,
                        MainConfig.server.channel_size_limit()),
                this.epoll);

        this.node.error_callback = &this.nodeError;

        this.sigint_event = new SignalEvent(&this.sigintHandler, [SIGINT]);

        this.periodics = new Periodics(this.node, this.epoll);
        this.periodics.add(new PeriodicStats());
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

        Trace.formatln("Starting event loop");
        this.epoll.eventLoop();
        Trace.formatln("Event loop exited");

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
            OceanException.Warn("OutOfMemoryException caught in eventLoop");
        }
        else
        {
            OceanException.Warn("Exception caught in eventLoop: '{}' @ {}:{}",
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

