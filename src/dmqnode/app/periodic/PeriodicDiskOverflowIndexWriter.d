/*******************************************************************************

    Writes the disk overflow periodically.

    copyright: Copyright (c) 2015 sociomantic labs. All rights reserved

*******************************************************************************/

module dmqnode.app.periodic.PeriodicDiskOverflowIndexWriter;


import dmqnode.app.periodic.model.IPeriodic;

/*******************************************************************************

    Periodic write buffer flusher.

*******************************************************************************/

public class PeriodicDiskOverflowIndexWriter : IPeriodic
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

        Called once update period by the base class.  Writes the disk overfow
        index.

    ***************************************************************************/

    override protected void run ( )
    {
        this.node.writeDiskOverflowIndex();
    }
}
