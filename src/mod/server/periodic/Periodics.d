/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012: Initial release

    authors:        Gavin Norman

    TODO: description of module

*******************************************************************************/

module src.mod.server.periodic.Periodics;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.server.periodic.model.IPeriodic;

private import ocean.io.select.EpollSelectDispatcher;

private import swarm.core.node.model.INode;
private import swarm.queue.node.model.IQueueNodeInfo;



public class Periodics
{
    private IPeriodic[] periodics;

    private const INode node;

    public this ( INode node )
    {
        this.node = node;
    }

    public void add ( IPeriodic periodic )
    {
        periodic.setNode(this.node);
        this.periodics ~= periodic;
    }

    public void shutdown ( EpollSelectDispatcher epoll )
    {
        foreach ( periodic; this.periodics )
        {
            epoll.unregister(periodic);
        }
    }

    public void register ( EpollSelectDispatcher epoll )
    {
        foreach ( periodic; this.periodics )
        {
            epoll.register(periodic);
        }
    }
}

