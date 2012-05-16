/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012: Initial release

    authors:        Gavin Norman

    Class which manages a set of periodically firing maintenance tasks over the
    dht node.

    TODO: this is a replica of the same module in the QueueNode project. These
    should be placed somewhere central so they can be shared.

*******************************************************************************/

module src.mod.node.periodic.Periodics;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.node.periodic.model.IPeriodic;

private import ocean.io.select.EpollSelectDispatcher;

private import swarm.dht.node.model.IDhtNode;
private import swarm.dht.node.model.IDhtNodeInfo;



/*******************************************************************************

    Periodics manager. Handles the registration and shutdown of a set of
    periodically firing maintenance tasks over the dht node.

*******************************************************************************/

public class Periodics
{
    /***************************************************************************

        Set of active periodics.

    ***************************************************************************/

    private IPeriodic[] periodics;


    /***************************************************************************

        Interface to dht node.

    ***************************************************************************/

    private const IDhtNode dht_node;


    /***************************************************************************

        Epoll select dispatcher used by periodcs' timer events.

    ***************************************************************************/

    private const EpollSelectDispatcher epoll;


    /***************************************************************************

        Constructor.

        Params:
            dht_node = interface to dht node
            epoll = epoll select dispatcher to register periodics with

    ***************************************************************************/

    public this ( IDhtNode dht_node, EpollSelectDispatcher epoll )
    {
        this.dht_node = dht_node;
        this.epoll = epoll;
    }


    /***************************************************************************

        Adds a periodic to the set.

        Params:
            periodic = new periodic to add

    ***************************************************************************/

    public void add ( IPeriodic periodic )
    {
        periodic.setDhtNode(this.dht_node);
        this.periodics ~= periodic;
    }


    /***************************************************************************

        Registers all periodics with epoll.

    ***************************************************************************/

    public void register ( )
    {
        foreach ( periodic; this.periodics )
        {
            this.epoll.register(periodic);
        }
    }


    /***************************************************************************

        Shuts down all periodics, unregistering them from epoll.

    ***************************************************************************/

    public void shutdown ( )
    {
        foreach ( periodic; this.periodics )
        {
            this.epoll.unregister(periodic);
        }
    }
}

