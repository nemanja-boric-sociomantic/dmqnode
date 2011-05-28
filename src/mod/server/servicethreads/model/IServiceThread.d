/*******************************************************************************

    Queue node service thread abstract base class

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.server.servicethreads.model.IServiceThread;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.server.util.Terminator;

private import ocean.util.OceanException;

private import tango.core.Thread;

private import swarm.queue.node.model.IQueueNode,
               swarm.queue.node.model.IQueueNodeInfo;

debug private import tango.util.log.Trace;



public abstract class IServiceThread : Thread
{
    /***************************************************************************

        Informational interface to the queue node.

    ***************************************************************************/

    private IQueueNodeInfo node_info;


    /***************************************************************************

        Time to sleep between runs of the service.

    ***************************************************************************/

    private uint update_time;


    /***************************************************************************

        Constructor.
        
        Params:
            queue = queue node to service
            update_time = time to sleep between runs of the service

    ***************************************************************************/

    public this ( IQueueNode queue, uint update_time )
    {
        this.update_time = update_time;

        this.node_info = queue.nodeInfo();

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
            try
            {
                Thread.sleep(this.update_time);

                if ( !Terminator.terminating )
                {
                    this.serviceNode(this.node_info, this.update_time);
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

    abstract protected void serviceNode ( IQueueNodeInfo node_info, uint seconds_elapsed );


    /***************************************************************************

        Gets the identifying string for the sub-class (used for message
        printing).
    
        Returns:
            sub-class id
    
    ***************************************************************************/

    abstract protected char[] id ( );
}

