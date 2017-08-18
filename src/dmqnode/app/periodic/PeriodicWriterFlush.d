/*******************************************************************************

    Periodic to flush the write buffers of the stream requests the node is
    handling.

    copyright:
        Copyright (c) 2012-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.app.periodic.PeriodicWriterFlush;


import dmqnode.app.periodic.model.IPeriodic;

/*******************************************************************************

    Periodic write buffer flusher.

*******************************************************************************/

public class PeriodicWriterFlush : IPeriodic
{
    /***************************************************************************

        Constructor.

        Params:
            node = DMQ node
            epoll = epoll select dispatcher to register this periodic with (the
                registration of periodics is usually dealt with by the Periodics
                class, but an individual periodic can also reregister itself
                with epoll in the situation where an error occurs)
            period_ms = milliseconds between calls to handle()

    ***************************************************************************/

    public this ( DmqNode node, EpollSelectDispatcher epoll, uint period_ms )
    {
        super(node, epoll, period_ms, typeof(this).stringof);
    }


    /***************************************************************************

        Called once update period by the base class. Flushes the node.

    ***************************************************************************/

    override protected void run ( )
    {
        this.node.flush();
    }
}
