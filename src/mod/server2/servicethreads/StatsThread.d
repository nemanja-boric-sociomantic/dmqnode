/*******************************************************************************

    Dht node stats thread. Outputs info about the performance of the dht node to
    a trace log at intervals.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        February 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module mod.server2.servicethreads.StatsThread;



/*******************************************************************************

    Imports

*******************************************************************************/

private import mod.server2.servicethreads.model.IServiceThread;

private import ocean.util.TraceLog;

private import swarm.dht2.node.model.IDhtNode,
               swarm.dht2.node.model.IDhtNodeInfo;

private import swarm.dht2.storage.model.IStorageEngineService;

debug private import tango.util.log.Trace;



class StatsThread : IServiceThread
{
    /***************************************************************************

        Constructor.
        
        Params:
            dht = dht node to service
            update_time = time to sleep between runs of the service
    
    ***************************************************************************/

    public this ( IDhtNode dht, uint update_time )
    {
        super(dht, update_time);
    }


    /***************************************************************************

        Method called on the node info interface once per service run. Outputs
        stats info to the trace log.

        Params:
            node_info = node information interface
            seconds_elapsed = time since this service was last performed
    
    ***************************************************************************/

    protected void serviceNode ( IDhtNodeInfo node_info, uint seconds_elapsed )
    {
        auto received = node_info.bytesReceived;
        auto sent = node_info.bytesSent;
        TraceLog.write("Node stats: {} sent ({} K/s), {} received ({} K/s), handling {} connections",
                sent, cast(float)(sent / 1024) / cast(float)seconds_elapsed,
                received, cast(float)(received / 1024) / cast(float)seconds_elapsed,
                node_info.numOpenConnections);
        node_info.resetByteCounters();
    }


    /***************************************************************************

        Method called on the channel service interface of all storage channels
        once per service run. Does nothing (required by base class).
    
        Params:
            channel = channel service interface
            seconds_elapsed = time since this service was last performed
    
    ***************************************************************************/

    protected void serviceChannel ( IStorageEngineService channel, uint seconds_elapsed )
    {
    }


    /***************************************************************************

        Gets the identifying string for this class (used for message printing).
    
        Returns:
            class id
    
    ***************************************************************************/
    
    protected char[] id ( )
    {
        return typeof(this).stringof;
    }
}

