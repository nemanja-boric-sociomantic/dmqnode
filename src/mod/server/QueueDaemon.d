/*******************************************************************************

        Queue Node Server Daemon

        copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved

        version:        October 2010: Initial release

        authors:        David Eckardt, Gavin Norman 
                        Thomas Nicolai, Lars Kirchhoff

 ******************************************************************************/

module mod.server.DhtDaemon;



/*******************************************************************************

    Imports

 ******************************************************************************/

private import  core.config.MainConfig;

private import  mod.server.servicethreads.ServiceThreads,
                mod.server.servicethreads.StatsThread;

private import  swarm.queue.QueueNode;

private import  swarm.queue.QueueConst;

private import  swarm.queue.node.model.IQueueNode;

private import  swarm.queue.storage.Ring;

private import  ocean.util.Config;

private import  ocean.core.Exception: assertEx;

private import  tango.core.Exception: IllegalArgumentException;

private import	tango.util.log.Log, tango.util.log.AppendConsole;

debug private import tango.util.log.Trace;

/*******************************************************************************

    QueueDaemon

 ******************************************************************************/

class QueueDaemon
{
    /***************************************************************************

        Queue node type alias
    
     **************************************************************************/

    private alias QueueNode!(Ring, uint) Queue;


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
        uint size_limit = Config.get!(uint)("Server", "size_limit");
        uint channel_size_limit = Config.get!(uint)("Server", "channel_size_limit");
        char[] data_dir = Config.get!(char[])("Server", "data_dir");

        assertEx!(IllegalArgumentException)(size_limit, "size limit 0 specified in configuration");

        this.node = new Queue(
                QueueConst.NodeItem(Config.Char["Server", "address"], Config.Int["Server", "port"]),
                size_limit, channel_size_limit, data_dir, channel_size_limit);

        debug Trace.formatln("Queue node: {}:{}", Config.Char["Server", "address"], Config.Char["Server", "port"]);

        this.service_threads = new ServiceThreads;
        this.service_threads.add(new StatsThread(this.node, Config.Int["ServiceThreads", "stats_sleep"]));
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

