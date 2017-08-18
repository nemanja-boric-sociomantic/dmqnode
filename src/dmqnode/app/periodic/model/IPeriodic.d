/*******************************************************************************

    Abstract base class for periodics -- routines which should be invoked
    periodically using an epoll timer.

    copyright:
        Copyright (c) 2012-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.app.periodic.model.IPeriodic;


import dmqnode.app.util.Terminator;
import dmqnode.node.DmqNode;
import dmqnode.node.IDmqNodeInfo;

import ocean.io.select.client.TimerEvent;
import ocean.io.select.EpollSelectDispatcher;
import ocean.util.log.Log;


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

    private char[] id;


    /***************************************************************************

        Epoll select dispatcher used by the periodic's timer event.

    ***************************************************************************/

    private EpollSelectDispatcher epoll;


    /***************************************************************************

        Constructor.

        Params:
            node = DMQ node
            epoll = epoll select dispatcher to register this periodic with (the
                registration of periodics is usually dealt with by the Periodics
                class, but an individual periodic can also reregister itself
                with epoll in the situation where an error occurs)
            period_ms = milliseconds between calls to handle()
            id = identifying string of this periodic, used for logging

    ***************************************************************************/

    public this ( DmqNode node, EpollSelectDispatcher epoll, uint period_ms, char[] id )
    {
        this.node_info = this.node = node;

        auto s = period_ms / 1000;
        auto ms = (period_ms) % 1000;
        this.set(s, ms, s, ms);

        this.id = id;
        this.epoll = epoll;
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
