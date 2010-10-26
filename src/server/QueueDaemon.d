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

        Queue node object

     **************************************************************************/

    private     IQueueNode                node;

    /***************************************************************************

         Constructor

     **************************************************************************/

    public this ( )
    {
        this.setLogger();

        uint    number_threads  = Config.get!(uint)("Server", "connection_threads");
        uint    size_limit      = Config.get!(uint)("Server", "size_limit");
        char[]  data_dir        = Config.get!(char[])("Server", "data_dir");
        
        assertEx!(IllegalArgumentException)(number_threads, "number of threads of 0 specified in configuration");
        assertEx!(IllegalArgumentException)(size_limit,     "size limit 0 specified in configuration");
        
        this.node = new QueueNode!(Ring, uint)(QueueConst.NodeItem(Config.getChar("Server", "address"),
                                                                   Config.getInt("Server", "port")),
                                               number_threads, data_dir, size_limit);
    }

    /***************************************************************************

        Runs the queue node

     **************************************************************************/

    public int run ()
    {
        this.node.run();
        this.node.attach();

        return true;
    }

    /***************************************************************************

        Shuts down the queue node

     **************************************************************************/

    public void shutdown ( )
    {
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