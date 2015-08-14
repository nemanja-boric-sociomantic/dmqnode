/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012: Initial release
                    30/05/2013: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

    Class which manages a set of periodically firing maintenance tasks over the
    swarm node.

*******************************************************************************/

module dmqnode.app.periodic.Periodics;



/*******************************************************************************

    Imports

*******************************************************************************/

private import dmqnode.app.periodic.model.IPeriodic;

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

    private const IPeriodic.DmqNode node;


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

    public this ( IPeriodic.DmqNode node, EpollSelectDispatcher epoll )
    {
        this.node  = node;
        this.epoll = epoll;
    }


    /***************************************************************************

        Adds a new Periodic instance to the set. The Periodic constructor is
        expected to accept the following argument list:
        ---
          (DmqNode node, EpollSelectDispatcher epoll, ctor_args)
        ---

        Params:
            ctor_args = additional Periodic constructor arguments

    ***************************************************************************/

    public void add ( Periodic: IPeriodic, CtorArgs ...  ) ( CtorArgs ctor_args )
    {
        this.periodics ~= new Periodic(this.node, this.epoll, ctor_args);
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

