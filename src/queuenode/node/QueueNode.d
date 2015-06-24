/*******************************************************************************

    Queue Node Implementation

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2009: Initial release
                    April 2011:     Asynchronous version

    authors:        Gavin Norman

*******************************************************************************/

module queuenode.node.QueueNode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.model.ChannelsNode : ChannelsNodeBase;

private import queuenode.node.IQueueNodeInfo;

private import queuenode.connection.QueueConnectionHandler;

private import queuenode.storage.model.QueueStorageEngine;
private import queuenode.storage.model.QueueStorageChannels;

private import swarm.dmq.DmqConst;

private import queuenode.connection.SharedResources;

private import ocean.io.select.EpollSelectDispatcher;



/*******************************************************************************

    QueueNode

*******************************************************************************/

public class QueueNode
    : ChannelsNodeBase!(QueueStorageEngine, QueueConnectionHandler), IQueueNodeInfo
{
    /***************************************************************************

        Constructor

        Params:
            node_item = node address & port
            channels = storage channels instance
            epoll = epoll select dispatcher to be used internally
            backlog = (see ISelectListener ctor)

    ***************************************************************************/

    public this ( DmqConst.NodeItem node_item, QueueStorageChannels channels,
        EpollSelectDispatcher epoll, int backlog )
    {
        auto conn_setup_params = new QueueConnectionSetupParams;
        conn_setup_params.node_info = this;
        conn_setup_params.epoll = epoll;
        conn_setup_params.storage_channels = channels;
        conn_setup_params.shared_resources = new SharedResources;

        super(node_item, channels, conn_setup_params, backlog);
    }


    /***************************************************************************

        Returns:
            information interface to this node

    ***************************************************************************/

    public IQueueNodeInfo node_info ( )
    {
        return this;
    }


    /***************************************************************************

        Returns:
            maximum number of bytes per channel

    ***************************************************************************/

    public ulong channelSizeLimit ( )
    {
        return (cast(QueueStorageChannels)super.channels).channelSizeLimit;
    }


    /***************************************************************************

        Returns:
            identifier string for this node

    ***************************************************************************/

    override protected char[] id ( )
    {
        return typeof(this).stringof;
    }
}

