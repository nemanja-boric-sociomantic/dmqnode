/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012: Initial release

    authors:        Gavin Norman

    Abstract base class for periodics -- routines which should be invoked
    periodically using an epoll timer.

*******************************************************************************/

module src.mod.node.periodic.model.IPeriodic;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.io.select.event.TimerEvent;

private import ocean.io.select.EpollSelectDispatcher;

private import src.mod.node.util.Terminator;

private import swarm.core.node.model.INode;
private import swarm.queue.node.model.IQueueNodeInfo;



/*******************************************************************************

    Periodic base class.

*******************************************************************************/

public abstract class IPeriodic : TimerEvent
{
    /***************************************************************************

        Local alias redefinitions for derived classes

    ***************************************************************************/

    protected alias .INode INode;
    protected alias .IQueueNodeInfo IQueueNodeInfo;


    /***************************************************************************

        Interface to a node.

    ***************************************************************************/

    protected INode node;


    /***************************************************************************

        Constructor.

        Params:
            period_s = seconds between calls to handle()

    ***************************************************************************/

    public this ( uint period_s )
    {
        super(&this.handle);

        this.set(period_s, 0, period_s, 0);
    }


    /***************************************************************************

        Passes a queue node interface to the periodic. The interface is passed
        via this method rather than the constructor so that a single instance of
        the class Periodics can be instantiated and passed a queue node
        interface which it will in turn pass through to all IPeriodic instances
        which it owns.

        Params:
            queue_node = queue node interface to use

    ***************************************************************************/

    public void setNode ( INode node )
    {
        this.node = node;
    }


    /***************************************************************************

        Timer callback. If the termination (SIGINT) signal has not been
        received, then the abstract handle_() method is called and the periodic
        is registered back with epoll to fire again. If termination has been
        requested, then the method does nothing and simply returns false to
        unregister from epoll.

    ***************************************************************************/

    private bool handle ( )
    {
        assert(this.node !is null);

        if ( !Terminator.terminating )
        {
            this.handle_();
        }

        return !Terminator.terminating; // unless terminating, always stay registered to fire again periodically
    }


    /***************************************************************************

        Abstract handle method which should implement the desired behaviour of
        the periodic.

    ***************************************************************************/

    protected abstract void handle_ ( );
}

