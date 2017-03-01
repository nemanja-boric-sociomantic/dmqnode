/*******************************************************************************

    Distributed Message Queue Node Implementation

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

*******************************************************************************/

module dmqnode.node.DmqNode;


import dmqnode.app.config.ChannelSizeConfig;
import dmqnode.app.config.ServerConfig;
import dmqnode.connection.ConnectionHandler;
import Neo = dmqnode.connection.neo.SharedResources;
import dmqnode.connection.SharedResources;
import dmqnode.node.IDmqNodeInfo;
import dmqnode.node.RequestHandlers;
import dmqnode.storage.model.StorageChannels;
import dmqnode.storage.model.StorageEngine;
import dmqnode.storage.Ring;

import swarm.core.neo.authentication.HmacDef: Key;
import swarm.core.node.model.NeoChannelsNode : ChannelsNodeBase;
import swarm.core.node.storage.model.IStorageEngineInfo;
import swarm.dmq.DmqConst;

import ocean.io.select.EpollSelectDispatcher;



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
                  EpollSelectDispatcher epoll, bool no_delay )
    {
        auto ringnode = new RingNode(server_config.data_dir, this,
                                     server_config.size_limit,
                                     channel_size_config);

        // Classic connection handler settings
        auto conn_setup_params = new ConnectionSetupParams;
        conn_setup_params.node_info = this;
        conn_setup_params.epoll = epoll;
        conn_setup_params.storage_channels = ringnode;
        conn_setup_params.shared_resources = new SharedResources;

        // Neo node / connection handler settings
        Options options;
        options.epoll = epoll;
        options.cmd_handlers = request_handlers;
        options.shared_resources = new Neo.SharedResources(ringnode);
        options.no_delay = no_delay;
        options.unix_socket_path = server_config.unix_socket_path();
        options.credentials_filename = "etc/credentials";

        super(DmqConst.NodeItem(server_config.address(), server_config.port()),
            server_config.neoport(), ringnode, conn_setup_params, options,
            server_config.backlog);
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
