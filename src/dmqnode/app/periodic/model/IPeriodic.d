/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012: Initial release
                    30/05/30: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

    Abstract base class for periodics -- routines which should be invoked
    periodically using an epoll timer.

*******************************************************************************/

module dmqnode.app.periodic.model.IPeriodic;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.io.select.client.TimerEvent;

private import ocean.io.select.EpollSelectDispatcher;

private import dmqnode.app.util.Terminator;

private import dmqnode.node.DmqNode,
               dmqnode.node.IDmqNodeInfo;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger logger;
static this ( )
{
    logger = Log.lookup("dmqnode.app.model.IPeriodic");
}



/*******************************************************************************

    Periodic base class.

*******************************************************************************/

public abstract class IPeriodic : ITimerEvent
{
    /***************************************************************************

        Local alias redefinitions for derived classes

    ***************************************************************************/

    protected alias .DmqNode DmqNode;

    protected alias .IDmqNodeInfo IDmqNodeInfo;

    protected alias .EpollSelectDispatcher EpollSelectDispatcher;


    /***************************************************************************

        Interface to a swarm node.

    ***************************************************************************/

    protected DmqNode node;
    protected IDmqNodeInfo node_info;


    /***************************************************************************

        Identifying string of this periodic, used for logging.

    ***************************************************************************/

    private const char[] id;


    /***************************************************************************

        Epoll select dispatcher used by the periodic's timer event.

    ***************************************************************************/

    private const EpollSelectDispatcher epoll;


    /***************************************************************************

        Constructor.

        Params:
            epoll = epoll select dispatcher to register this periodic with (the
                registration of periodics is usually dealt with by the Periodics
                class, but an individual periodic can also reregister itself
                with epoll in the situation where an error occurs)
            period_ms = milliseconds between calls to handle()
            id = identifying string of this periodic, used for logging

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, uint period_ms, char[] id )
    {
        auto s = period_ms / 1000;
        auto ms = (period_ms) % 1000;
        this.set(s, ms, s, ms);

        this.id = id;
        this.epoll = epoll;
    }


    /***************************************************************************

        Passes a swarm node interface to the periodic. The interface is passed via
        this method rather than the constructor so that a single instance of the
        class Periodics can be instantiated and passed a swarm node interface
        which it will in turn pass through to all IPeriodic instances which it
        owns.

        Params:
            node = swarm node interface to use

    ***************************************************************************/

    public void setNode ( DmqNode node )
    {
        this.node_info = this.node = node;
    }


    /***************************************************************************

        Timer callback. If the termination (SIGINT) signal has not been
        received, then the abstract handle_() method is called and the periodic
        is registered back with epoll to fire again. If termination has been
        requested, then the method does nothing and simply returns false to
        unregister from epoll.

        Params:
            n =  number of  expirations that have occurred

        Returns:
            true to stay registered in epoll or false to unregister.

    ***************************************************************************/

    override protected bool handle_ ( ulong n )
    {
        assert(this.node !is null);
        assert(this.node_info !is null);

        if ( !Terminator.terminating )
        {
            this.run();
        }

        return !Terminator.terminating; // unless terminating, always stay registered to fire again periodically
    }


    /***************************************************************************

        Abstract handle method which should implement the desired behaviour of
        the periodic.

    ***************************************************************************/

    protected abstract void run ( );


    /***************************************************************************

        Error reporting method, called when an Exception is caught from
        handle().

        Params:
            exception: Exception thrown by handle()
            event:     Seletor event while exception was caught

    ***************************************************************************/

    override protected void error_ ( Exception exception, Event event )
    {
        logger.error("Exception caught in timer handler for {}: {} @ {}:{}",
            this.id, exception.msg, exception.file, exception.line);
    }


    /***************************************************************************

        Finalize method, called after this instance has been unregistered from
        epoll.

    ***************************************************************************/

    public override void finalize ( FinalizeStatus status )
    {
        logger.error("Timer handler {} unregistered", this.id);
        if ( status != status.Success )
        {
            logger.error("Reregistering timer handler {}", this.id);
            this.epoll.register(this);
        }
    }
}

