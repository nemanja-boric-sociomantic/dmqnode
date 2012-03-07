/*******************************************************************************

        Queue Node Server

        copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

        version:        October 2010: Initial release

        authors:        David Eckardt, Gavin Norman 
                        Thomas Nicolai, Lars Kirchhoff

 ******************************************************************************/

module src.mod.server.QueueServer;



/*******************************************************************************

    Imports

 ******************************************************************************/

private import src.mod.server.config.MainConfig;

private import src.mod.server.util.Terminator;

private import src.mod.server.servicethreads.ServiceThreads,
               src.mod.server.servicethreads.StatsThread;

private import swarm.queue.QueueNode;

private import swarm.queue.QueueConst;

private import swarm.queue.node.storage.Ring;

private import ocean.core.MessageFiber;
private import ocean.io.select.protocol.generic.ErrnoIOException : IOWarning;

private import ocean.io.select.EpollSelectDispatcher;

private import ocean.io.select.event.SignalEvent;

private import ocean.core.Exception : assertEx;

private import ocean.io.select.model.ISelectClient;

private import ocean.util.OceanException;

debug private import ocean.util.log.Trace;

private import tango.core.Exception : IllegalArgumentException, OutOfMemoryException;

private import tango.stdc.posix.signal: SIGINT;



/*******************************************************************************

    QueueServer

 ******************************************************************************/

public class QueueServer
{
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

        Service threads handler

    **************************************************************************/

    private ServiceThreads service_threads;


    /***************************************************************************

         Constructor

     **************************************************************************/

    public this ( )
    {
        this.epoll = new EpollSelectDispatcher;

        this.node = new QueueNode(
                QueueConst.NodeItem(MainConfig.server.address(),
                    MainConfig.server.port()),
                new RingNode(MainConfig.server.data_dir, MainConfig.server.size_limit,
                        MainConfig.server.channel_size_limit()),
                this.epoll);

        this.node.error_callback = &this.nodeError;

        this.sigint_event = new SignalEvent(&this.sigintHandler, [SIGINT]);

        this.node.error_callback = &this.nodeError;

        this.service_threads = new ServiceThreads;
        if ( MainConfig.log.stats_log_enabled || MainConfig.log.console_stats_enabled )
        {
            this.service_threads.add(new StatsThread(this.node.node_info, MainConfig.log.stats_log_period));
        }
    }


    /***************************************************************************

        Runs the queue node

     **************************************************************************/

    public int run ( )
    {
        this.service_threads.start();

        this.epoll.register(this.sigint_event);

//        this.periodics.register(this.epoll);

        this.node.register(this.epoll);

        Trace.formatln("Starting event loop");
        this.epoll.eventLoop();
        Trace.formatln("Event loop exited");

        return true;
    }


    /***************************************************************************

        Shuts down the queue node

     **************************************************************************/

    public void shutdown ( )
    {
        this.node.shutdown();
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

//        this.periodics.shutdown(this.epoll);

        this.node.shutdown;

        this.epoll.shutdown;
    }
}

