/*******************************************************************************

    Dht node service thread abstract base class

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        February 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.node.servicethreads.model.IServiceThread;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.node.util.Terminator;

private import ocean.util.OceanException;

private import tango.core.Thread;

private import swarm.dht.node.model.IDhtNode,
               swarm.dht.node.model.IDhtNodeInfo;

private import swarm.dht.node.storage.model.IStorageChannelsService;

private import swarm.dht.node.storage.model.IStorageEngineService;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Service thread abstract base class.

*******************************************************************************/

abstract public class IServiceThread : Thread
{
    /***************************************************************************

        Aliases for sub-classes to use.

    ***************************************************************************/

    protected alias .IStorageChannelsService IStorageChannelsService;

    protected alias .IStorageEngineService IStorageEngineService;

    
    /***************************************************************************

        Delegate to be called on thread exit.

    ***************************************************************************/

    public void delegate ( char[] id ) finished_callback;


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

        this.node_info = cast(IDhtNodeInfo)dht;

        this.channels_service = dht.channelsService();

        super(&this.run);
    }


    /***************************************************************************

        Thread main method. Repeatedly sleeps then calls the service methods on
        the node and on each channel in the node. The finished delegate is
        called last thing.

    ***************************************************************************/

    private void run ( )
    {
        scope ( exit )
        {
            if ( this.finished_callback !is null )
            {
                this.finished_callback(this.id);
            }
        }

        while ( !Terminator.terminating )
        {
            try
            {
                for ( int i; i < this.update_time && !Terminator.terminating; i++ )
                {
                    Thread.sleep(1);
                }

                if ( !Terminator.terminating )
                {
                    this.serviceNode(this.node_info, this.update_time);

                    foreach ( channel; this.channels_service )
                    {
                        this.serviceChannel(channel, this.update_time);
                    }
                }
            }
            catch ( Exception e )
            {
                OceanException.Warn("Error during {}: {}", this.id, e.msg);
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


    /***************************************************************************

        Gets the identifying string for the sub-class (used for message
        printing).
    
        Returns:
            sub-class id
    
    ***************************************************************************/

    abstract protected char[] id ( );
}

