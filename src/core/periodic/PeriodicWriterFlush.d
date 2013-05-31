/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        04/09/2012: Initial release
                    30/05/2013: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

    Periodic to flush the write buffers of the stream requests the node is
    handling.

*******************************************************************************/

module src.core.periodic.PeriodicWriterFlush;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.core.periodic.model.IPeriodic;



/*******************************************************************************

    Periodic write buffer flusher.

*******************************************************************************/

public class PeriodicWriterFlush : IPeriodic
{
    /***************************************************************************

        Constructor.

    ***************************************************************************/

    public this ( uint period_ms )
    {
        super(period_ms);
    }


    /***************************************************************************

        Called once update period by the base class. Flushes the node.

    ***************************************************************************/

    protected void handle_ ( )
    {
        this.node.flush();
    }
}

