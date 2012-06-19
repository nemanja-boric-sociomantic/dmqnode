/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012: Initial release

    authors:        Gavin Norman

    Class which manages a set of periodically firing maintenance tasks over the
    queue node.

    TODO: this is a replica of the same module in the DhtNode project. These
    should be placed somewhere central so they can be shared.

*******************************************************************************/

module src.core.periodic.Periodics;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.core.periodic.model.IPeriodic;

private import ocean.io.select.EpollSelectDispatcher;

private import swarm.core.node.model.INode;
private import swarm.queue.node.model.IQueueNodeInfo;



/*******************************************************************************

    Periodics manager. Handles the registration and shutdown of a set of
    periodically firing maintenance tasks over the queue node.

*******************************************************************************/

public class Periodics
{
    /***************************************************************************

        Set of active periodics.

    ***************************************************************************/

    private IPeriodic[] periodics;


    /***************************************************************************

        Interface to queue node.

    ***************************************************************************/

    private const INode node;


    /***************************************************************************

        Epoll select dispatcher used by periodcs' timer events.

    ***************************************************************************/

    private const EpollSelectDispatcher epoll;


    /***************************************************************************

        Constructor.

        Params:
            node = interface to queue node
            epoll = epoll select dispatcher to register periodics with

    ***************************************************************************/

    public this ( INode node, EpollSelectDispatcher epoll )
    {
        this.node = node;
        this.epoll = epoll;
    }


    /***************************************************************************

        Adds a periodic to the set.

        Params:
            periodic = new periodic to add

    ***************************************************************************/

    public void add ( IPeriodic periodic )
    {
        periodic.setNode(this.node);
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

