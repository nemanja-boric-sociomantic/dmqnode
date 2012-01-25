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

private import src.mod.server.servicethreads.ServiceThreads,
               src.mod.server.servicethreads.StatsThread;

private import swarm.queue.QueueNode;

private import swarm.queue.QueueConst;

private import swarm.queue.node.storage.Ring;

private import ocean.core.MessageFiber;
private import ocean.io.select.protocol.generic.ErrnoIOException : IOWarning;

private import ocean.core.Exception : assertEx;

private import ocean.io.select.model.ISelectClient;

private import ocean.util.OceanException;

debug private import ocean.util.log.Trace;

private import tango.core.Exception : IllegalArgumentException, OutOfMemoryException;



/*******************************************************************************

    QueueServer

 ******************************************************************************/

public class QueueServer
{
    /***************************************************************************

        Queue node instance

     **************************************************************************/

    private QueueNode node;


    /***************************************************************************

        Service threads handler

    **************************************************************************/

    private ServiceThreads service_threads;


    /***************************************************************************

         Constructor

     **************************************************************************/

    public this ( )
    {
        assertEx!(IllegalArgumentException)(MainConfig.server.size_limit, "size limit 0 specified in configuration");

        this.node = new QueueNode(
                QueueConst.NodeItem(MainConfig.server.address(), MainConfig.server.port()),
                new RingNode(MainConfig.server.data_dir, MainConfig.server.size_limit,
                        MainConfig.server.channel_size_limit));

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

    public int run ()
    {
        this.service_threads.start();

        this.node.eventLoop();

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
}

