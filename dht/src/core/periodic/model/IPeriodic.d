/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012: Initial release

    authors:        Gavin Norman

    Abstract base class for periodics -- routines which should be invoked
    periodically using an epoll timer.

*******************************************************************************/

module src.core.periodic.model.IPeriodic;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.io.select.event.TimerEvent;

private import ocean.io.select.EpollSelectDispatcher;

private import src.core.util.Terminator;

private import swarm.core.node.model.INode;
private import swarm.dht.node.model.IDhtNodeInfo;



/*******************************************************************************

    Periodic base class.

*******************************************************************************/

public abstract class IPeriodic : TimerEvent
{
    /***************************************************************************

        Local alias redefinitions for derived classes

    ***************************************************************************/

    protected alias .INode INode;
    protected alias .IDhtNodeInfo IDhtNodeInfo;


    /***************************************************************************

        Interface to a dht node.

    ***************************************************************************/

    protected INode dht_node;


    /***************************************************************************

        Constructor.

        Params:
            period_ms = milliseconds between calls to handle()

    ***************************************************************************/

    public this ( uint period_ms )
    {
        super(&this.handle);

        auto s = period_ms / 1000;
        auto ms = (period_ms) % 1000;
        this.set(s, ms, s, ms);
    }


    /***************************************************************************

        Passes a dht node interface to the periodic. The interface is passed via
        this method rather than the constructor so that a single instance of the
        class Periodics can be instantiated and passed a dht node interface
        which it will in turn pass through to all IPeriodic instances which it
        owns.

        Params:
            dht_node = dht node interface to use

    ***************************************************************************/

    public void setDhtNode ( INode dht_node )
    {
        this.dht_node = dht_node;
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
        assert(this.dht_node !is null);

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

