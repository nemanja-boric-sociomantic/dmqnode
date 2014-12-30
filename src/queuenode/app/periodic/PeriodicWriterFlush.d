/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        04/09/2012: Initial release
                    30/05/2013: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

    Periodic to flush the write buffers of the stream requests the node is
    handling.

*******************************************************************************/

module queuenode.app.periodic.PeriodicWriterFlush;



/*******************************************************************************

    Imports

*******************************************************************************/

private import queuenode.app.periodic.model.IPeriodic;



/*******************************************************************************

    Periodic write buffer flusher.

*******************************************************************************/

public class PeriodicWriterFlush : IPeriodic
{
    /***************************************************************************

        Constructor.

        Params:
            epoll = epoll select dispatcher to register this periodic with (the
                registration of periodics is usually dealt with by the Periodics
                class, but an individual periodic can also reregister itself
                with epoll in the situation where an error occurs)
            period_ms = milliseconds between calls to handle()

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, uint period_ms )
    {
        super(epoll, period_ms, typeof(this).stringof);
    }


    /***************************************************************************

        Called once update period by the base class. Flushes the node.

    ***************************************************************************/

    protected void handle_ ( )
    {
        this.node.flush();
    }
}

