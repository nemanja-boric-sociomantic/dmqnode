/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012 2012: Initial release

    authors:        Gavin Norman

    Periodic maintenance of storage channels. What exactly this maintenance does
    depends on the type of dht node. (In the memory node it effects a periodic
    dump of the in-memory channels to disk.)

*******************************************************************************/

module src.mod.node.periodic.PeriodicMaintenance;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.node.periodic.model.IPeriodic;

private import ocean.io.Stdout;



/*******************************************************************************

    Periodic channel maintenance class.

*******************************************************************************/

public class PeriodicMaintenance : IPeriodic
{
    /***************************************************************************

        Constructor.

        Params:
            period_s = seconds between calls to handle()

    ***************************************************************************/

    public this ( uint period_s )
    {
        super(period_s);
    }


    /***************************************************************************

        Called once every period_s (as specified in the constructor) by the base
        class. Calls the maintenance() method of each storage channel.

    ***************************************************************************/

    protected void handle_ ( )
    {
        this.dht_node.maintenance;
    }
}

