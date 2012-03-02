/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012: Initial release

    authors:        Gavin Norman

    TODO: description of module

*******************************************************************************/

module src.mod.node.periodic.Periodics;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.node.periodic.model.IPeriodic;

private import ocean.io.select.EpollSelectDispatcher;

private import swarm.dht.node.model.IDhtNode;
private import swarm.dht.node.model.IDhtNodeInfo;



public class Periodics
{
    private IPeriodic[] periodics;

    private const IDhtNode dht_node;

    public this ( IDhtNode dht_node )
    {
        this.dht_node = dht_node;
    }

    public void add ( IPeriodic periodic )
    {
        periodic.setDhtNode(this.dht_node);
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

