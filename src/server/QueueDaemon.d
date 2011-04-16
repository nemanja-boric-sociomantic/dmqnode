/*******************************************************************************

        Queue Node Server Daemon

        copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved

        version:        October 2010: Initial release

        authors:        David Eckardt, Gavin Norman 
                        Thomas Nicolai, Lars Kirchhoff

 ******************************************************************************/

module server.DhtDaemon;



/*******************************************************************************

    Imports

 ******************************************************************************/

private import core.config.MainConfig;

private import  server.QueueTracer;

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

        Queue channel tracer object

     **************************************************************************/

    private QueueTracer qtrace;


    /***************************************************************************

         Constructor

     **************************************************************************/

    public this ( )
    {
        this.setLogger();

        uint size_limit = Config.get!(uint)("Server", "size_limit");
        uint channel_size_limit = Config.get!(uint)("Server", "channel_size_limit");
        char[] data_dir = Config.get!(char[])("Server", "data_dir");

        assertEx!(IllegalArgumentException)(size_limit,     "size limit 0 specified in configuration");

        auto queue = new Queue(
                QueueConst.NodeItem(Config.Char["Server", "address"], Config.Int["Server", "port"]),
                size_limit, channel_size_limit, data_dir, channel_size_limit);
        this.node = queue;

        debug Trace.formatln("Queue node: {}:{}", Config.Char["Server", "address"], Config.Char["Server", "port"]);

        if ( MainConfig.show_channel_trace )
        {
            this.qtrace = new QueueTracer(queue);
        }
    }


    /***************************************************************************

        Runs the queue node

     **************************************************************************/

    public int run ()
    {
        this.qtrace.start();

        this.node.eventLoop();

        return true;
    }


    /***************************************************************************

        Shuts down the queue node

     **************************************************************************/

    public void shutdown ( )
    {
        this.qtrace.terminate().join();

        return this.node.shutdown();
    }


    /***************************************************************************

        Set Logger

     **************************************************************************/

    private void setLogger ()
    {
        auto log = Log.getLogger("dht.persist");
        log.add(new AppendConsole);
        log.level = Level.Trace;
    }
}

