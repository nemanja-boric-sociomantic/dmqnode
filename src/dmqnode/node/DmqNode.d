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

private import swarm.core.node.model.NeoChannelsNode : ChannelsNodeBase;

private import dmqnode.node.IDmqNodeInfo;

private import dmqnode.connection.ConnectionHandler;
private import dmqnode.node.RequestHandlers;

private import dmqnode.storage.Ring;
private import dmqnode.storage.model.StorageEngine;
private import dmqnode.storage.model.StorageChannels;

private import dmqnode.app.config.ServerConfig;
private import dmqnode.app.config.ChannelSizeConfig;

private import swarm.core.node.storage.model.IStorageEngineInfo;

private import swarm.dmq.DmqConst;

private import Neo = dmqnode.connection.neo.ConnectionSetupParams;
private import dmqnode.connection.SharedResources;

private import ocean.io.select.EpollSelectDispatcher;

import swarm.core.neo.authentication.HmacDef: Key;

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
            channel_size_config = channel size configuration
            client_credentials = the client authentication keys by client names
            epoll = epoll select dispatcher to be used internally
            no_delay = toggle Nagle's algorithm (true = disabled, false =
                enabled) on the connection sockets

    ***************************************************************************/

    public this ( ServerConfig server_config,
                  ChannelSizeConfig channel_size_config,
                  Key[char[]] client_credentials,
                  EpollSelectDispatcher epoll, bool no_delay )
    {
        auto ringnode = new RingNode(server_config.data_dir, this,
                                     server_config.size_limit,
                                     channel_size_config);

        auto conn_setup_params = new ConnectionSetupParams;
        conn_setup_params.node_info = this;
        conn_setup_params.epoll = epoll;
        conn_setup_params.storage_channels = ringnode;
        conn_setup_params.shared_resources = new SharedResources;

        auto neo_conn_setup_params = new Neo.ConnectionSetupParams(
            epoll, ringnode, request_handlers, client_credentials, no_delay
        );

        super(DmqConst.NodeItem(server_config.address(), server_config.port()), server_config.neoport(),
              ringnode, conn_setup_params, neo_conn_setup_params, server_config.backlog);
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

