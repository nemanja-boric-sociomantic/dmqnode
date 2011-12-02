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
        assertEx!(IllegalArgumentException)(MainConfig.server.size_limit, "size limit 0 specified in configuration");

        this.node = new QueueNode(
                QueueConst.NodeItem(MainConfig.server.address, MainConfig.server.port),
                new RingNode(MainConfig.server.data_dir, MainConfig.server.size_limit,
                        MainConfig.server.channel_size_limit));

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
}

