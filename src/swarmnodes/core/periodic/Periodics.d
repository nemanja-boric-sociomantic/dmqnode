/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012: Initial release
                    30/05/2013: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

    Class which manages a set of periodically firing maintenance tasks over the
    swarm node.

*******************************************************************************/

module swarmnodes.core.periodic.Periodics;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.core.periodic.model.IPeriodic;

private import ocean.io.select.EpollSelectDispatcher;



/*******************************************************************************

    Periodics manager. Handles the registration and shutdown of a set of
    periodically firing maintenance tasks over the swarm node.

*******************************************************************************/

public class Periodics
{
    /***************************************************************************

        Set of active periodics.

    ***************************************************************************/

    private IPeriodic[] periodics;


    /***************************************************************************

        Interface to swarm node.

    ***************************************************************************/

    private const IPeriodic.INode node;


    /***************************************************************************

        Epoll select dispatcher used by periodics' timer events.

    ***************************************************************************/

    private const EpollSelectDispatcher epoll;


    /***************************************************************************

        Constructor.

        Params:
            node = interface to swarm node
            epoll = epoll select dispatcher to register periodics with

    ***************************************************************************/

    public this ( IPeriodic.INode node, EpollSelectDispatcher epoll )
    {
        this.node  = node;
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

