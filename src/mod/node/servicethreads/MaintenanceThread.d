/*******************************************************************************

    Dht node maintenance thread. Calls the maintenance method of all storage
    channels of a dht node at intervals.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        February 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.node.servicethreads.MaintenanceThread;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.node.servicethreads.model.IServiceThread;

private import swarm.dht.node.model.IDhtNode,
               swarm.dht.node.model.IDhtNodeInfo;

private import swarm.dht.node.storage.engine.model.IStorageEngineService;

debug private import ocean.util.log.Trace;



class MaintenanceThread : IServiceThread
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

        Method called on the node info interface once per service run. Does
        nothing (required by base class).
    
        Params:
            node_info = node information interface
            seconds_elapsed = time since this service was last performed
    
    ***************************************************************************/

    protected void serviceNode ( IDhtNodeInfo node_info, uint seconds_elapsed )
    {
    }


    /***************************************************************************

        Method called on the channel service interface of all storage channels
        once per service run. Calls the maintenance method of each channel.

        Params:
            channel = channel service interface
            seconds_elapsed = time since this service was last performed
    
    ***************************************************************************/

    protected void serviceChannel ( IStorageEngineService channel, uint seconds_elapsed )
    {
        channel.maintenance();
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

