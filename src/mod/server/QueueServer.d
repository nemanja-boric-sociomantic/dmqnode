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

private import  src.mod.server.config.MainConfig;

private import  src.mod.server.servicethreads.ServiceThreads,
                src.mod.server.servicethreads.StatsThread;

private import  swarm.queue.QueueNode;

private import  swarm.queue.QueueConst;

private import  swarm.queue.node.model.IQueueNode;

private import  swarm.queue.storage.Ring;

private import  ocean.core.Exception: assertEx;

debug private import ocean.util.log.Trace;

private import  tango.core.Exception: IllegalArgumentException;

private import	tango.util.log.Log, tango.util.log.AppendConsole;



/*******************************************************************************

    QueueServer

 ******************************************************************************/

class QueueServer
{
    /***************************************************************************

        Queue node type alias
    
     **************************************************************************/

    private alias QueueNode!(RingNode, char[]) Queue;


    /***************************************************************************

        Queue node object

     **************************************************************************/

    private IQueueNode node;


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

        this.node = new Queue(
                QueueConst.NodeItem(MainConfig.address, MainConfig.port),
                MainConfig.size_limit, MainConfig.channel_size_limit, MainConfig.data_dir);

        debug Trace.formatln("Queue node: {}:{}", MainConfig.address, MainConfig.port);

        this.service_threads = new ServiceThreads;
        if ( MainConfig.stats_log_enabled || MainConfig.console_stats_enabled )
        {
            this.service_threads.add(new StatsThread(this.node, MainConfig.stats_log_period));
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
        return this.node.shutdown();
    }
}

