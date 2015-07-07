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

private import dmqnode.storage.Ring;
private import dmqnode.storage.model.StorageEngine;
private import dmqnode.storage.model.StorageChannels;

private import dmqnode.app.config.ServerConfig;

private import swarm.core.node.storage.model.IStorageEngineInfo;

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
            config = server configuration
            epoll = epoll select dispatcher to be used internally

    ***************************************************************************/

    public this ( ServerConfig config, EpollSelectDispatcher epoll )
    {
        auto ringnode = new RingNode(config.data_dir, this, config.size_limit,
                                     config.channel_size_limit());

        auto conn_setup_params = new ConnectionSetupParams;
        conn_setup_params.node_info = this;
        conn_setup_params.epoll = epoll;
        conn_setup_params.storage_channels = ringnode;
        conn_setup_params.shared_resources = new SharedResources;

        super(DmqConst.NodeItem(config.address(), config.port()),
              ringnode, conn_setup_params, config.backlog);
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

    /***************************************************************************

        'foreach' iteration over the channels.

    ***************************************************************************/

    public int opApply ( int delegate ( ref RingNode.Ring channel ) dg )
    {
        return super.opApply((ref IStorageEngineInfo channel_)
        {
            auto channel = cast(RingNode.Ring)channel_;
            assert(channel);
            return dg(channel);
        });
    }

    /**************************************************************************

        Makes the super class create record action counters.

        Returns:
            the identifier for the record action counters to create.

     **************************************************************************/

    override protected char[][] record_action_counter_ids ( )
    {
        return ["pushed", "popped"];
    }
}

