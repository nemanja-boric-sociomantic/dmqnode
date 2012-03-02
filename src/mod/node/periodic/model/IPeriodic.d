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

private import swarm.dht.node.model.IDhtNode;
private import swarm.dht.node.model.IDhtNodeInfo;



/*******************************************************************************

    Periodic base class.

*******************************************************************************/

public abstract class IPeriodic : TimerEvent
{
    /***************************************************************************

        Local alias redefinitions for derived classes

    ***************************************************************************/

    protected alias .IDhtNode IDhtNode;
    protected alias .IDhtNodeInfo IDhtNodeInfo;


    /***************************************************************************

        Interface to a dht node.

    ***************************************************************************/

    protected IDhtNode dht_node;


    /***************************************************************************

        Number of seconds to wait between runs.

    ***************************************************************************/

    protected const uint period_s;


    /***************************************************************************

        Constructor.

        Params:
            period_s = seconds between calls to handle()

    ***************************************************************************/

    public this ( uint period_s )
    {
        super(&this.handle);

        this.period_s = period_s;

        this.set(period_s, 0, period_s, 0);
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

    public void setDhtNode ( IDhtNode dht_node )
    {
        this.dht_node = dht_node;
    }


    /***************************************************************************

        Registers this periodic with the provided epoll selector. The periodic
        will then fire periodically, as specified in the constructor.

        Params:
            epoll = epoll selector to register with

    ***************************************************************************/

    public void register ( EpollSelectDispatcher epoll )
    {
        epoll.register(this);
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

