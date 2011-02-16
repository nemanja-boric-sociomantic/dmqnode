/*******************************************************************************

    Dht node service thread abstract base class

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        February 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module mod.server2.servicethreads.model.IServiceThread;



/*******************************************************************************

    Imports

*******************************************************************************/

private import mod.server2.util.Terminator;

private import tango.core.Thread;

private import swarm.dht2.node.model.IDhtNode,
               swarm.dht2.node.model.IDhtNodeInfo;

private import swarm.dht2.storage.channels.model.IStorageChannelsService;

private import swarm.dht2.storage.model.IStorageEngineService;

debug private import tango.util.log.Trace;



public abstract class IServiceThread : Thread
{
    /***************************************************************************

        Service interface to the storage channels.
    
    ***************************************************************************/

    private IStorageChannelsService channels_service;


    /***************************************************************************

        Informational interface to the dht node.

    ***************************************************************************/

    private IDhtNodeInfo node_info;


    /***************************************************************************

        Time to sleep between runs of the service.

    ***************************************************************************/

    private uint update_time;


    /***************************************************************************

        Constructor.
        
        Params:
            dht = dht node to service
            update_time = time to sleep between runs of the service

    ***************************************************************************/

    public this ( IDhtNode dht, uint update_time )
    {
        this.update_time = update_time;

        this.node_info = dht.nodeInfo();

        this.channels_service = dht.channelsService();

        super(&this.run);
    }


    /***************************************************************************

        Thread main method. Repeatedly sleeps then calls the service methods on
        the node and on each channel in the node.

    ***************************************************************************/

    private void run ( )
    {
        while ( !Terminator.terminating )
        {
            Thread.sleep(this.update_time);

            this.serviceNode(this.node_info, this.update_time);

            foreach ( channel; this.channels_service )
            {
                this.serviceChannel(channel, this.update_time);
            }
        }
    }


    /***************************************************************************

        Method called on the node info interface once per service run.

        Params:
            node_info = node information interface
            seconds_elapsed = time since this service was last performed

    ***************************************************************************/

    abstract protected void serviceNode ( IDhtNodeInfo node_info, uint seconds_elapsed );


    /***************************************************************************

        Method called on the channel service interface of all storage channels
        once per service run.

        Params:
            channel = channel service interface
            seconds_elapsed = time since this service was last performed

    ***************************************************************************/

    abstract protected void serviceChannel ( IStorageEngineService channel, uint seconds_elapsed );
}

