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

private import ocean.core.Exception : assertEx;

debug private import ocean.util.log.Trace;

private import tango.core.Exception : IllegalArgumentException;



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
        assertEx!(IllegalArgumentException)(MainConfig.size_limit, "size limit 0 specified in configuration");

        this.node = new QueueNode(
                QueueConst.NodeItem(MainConfig.address, MainConfig.port),
                new RingNode(MainConfig.data_dir, MainConfig.size_limit, MainConfig.channel_size_limit));

        this.service_threads = new ServiceThreads;
        if ( MainConfig.stats_log_enabled || MainConfig.console_stats_enabled )
        {
            this.service_threads.add(new StatsThread(this.node.node_info, MainConfig.stats_log_period));
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
}

