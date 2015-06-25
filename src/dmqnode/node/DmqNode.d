/*******************************************************************************

    Distributed Message Queue Node Implementation

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2009: Initial release
                    April 2011:     Asynchronous version

    authors:        Gavin Norman

*******************************************************************************/

module dmqnode.node.DmqNode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.model.ChannelsNode : ChannelsNodeBase;

private import dmqnode.node.IDmqNodeInfo;

private import dmqnode.connection.ConnectionHandler;

private import dmqnode.storage.model.StorageEngine;
private import dmqnode.storage.model.StorageChannels;

private import swarm.dmq.DmqConst;

private import dmqnode.connection.SharedResources;

private import ocean.io.select.EpollSelectDispatcher;



/*******************************************************************************

    DmqNode

*******************************************************************************/

public class DmqNode
    : ChannelsNodeBase!(StorageEngine, ConnectionHandler), IDmqNodeInfo
{
    /***************************************************************************

        Constructor

        Params:
            node_item = node address & port
            channels = storage channels instance
            epoll = epoll select dispatcher to be used internally
            backlog = (see ISelectListener ctor)

    ***************************************************************************/

    public this ( DmqConst.NodeItem node_item, StorageChannels channels,
        EpollSelectDispatcher epoll, int backlog )
    {
        auto conn_setup_params = new ConnectionSetupParams;
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

    public IDmqNodeInfo node_info ( )
    {
        return this;
    }


    /***************************************************************************

        Returns:
            maximum number of bytes per channel

    ***************************************************************************/

    public ulong channelSizeLimit ( )
    {
        return (cast(StorageChannels)super.channels).channelSizeLimit;
    }


    /***************************************************************************

        Returns:
            maximum number of bytes per channel

    ***************************************************************************/

    public void writeDiskOverflowIndex ( )
    {
        (cast(StorageChannels)this.channels).writeDiskOverflowIndex();
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

