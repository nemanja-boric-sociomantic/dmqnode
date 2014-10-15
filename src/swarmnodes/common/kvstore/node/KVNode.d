/*******************************************************************************

    Distributed Hashtable Node Implementation

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2009: Initial release
                    December 2010: Asynchronous version
                    August 2011: Fibers version

    authors:        Lars Kirchhoff, Thomas Nicolai, David Eckardt, Gavin Norman

    Key/value node class derived from the node base classes in swarm.

*******************************************************************************/

module swarmnodes.common.kvstore.node.KVNode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.model.ChannelsNode : ChannelsNodeBase;

private import swarm.dht.DhtConst;

private import swarmnodes.common.kvstore.node.IKVNodeInfo;
private import swarmnodes.common.kvstore.node.KVHashRange;

private import swarmnodes.common.kvstore.connection.KVConnectionHandler;

private import swarmnodes.common.kvstore.connection.SharedResources;

private import swarmnodes.common.kvstore.storage.KVStorageEngine;
private import swarmnodes.common.kvstore.storage.KVStorageChannels;

private import ocean.io.select.EpollSelectDispatcher;

debug private import tango.io.Stdout : Stderr;



/*******************************************************************************

    KVNode

*******************************************************************************/

public class KVNode : ChannelsNodeBase!(KVStorageEngine, KVConnectionHandler),
    IKVNodeInfo
{
    /**************************************************************************

        Node minimum & maximum hash

    ***************************************************************************/

    private const KVHashRange hash_range;


    /***************************************************************************

        Constructor.

        Params:
            node_item = node address/port
            channels = storage channels instance to use
            hash_range = min/max hash range tracker
            epoll = epoll select dispatcher to be used internally
            backlog = (see ISelectListener ctor)

    ***************************************************************************/

    public this ( DhtConst.NodeItem node_item, KVStorageChannels channels,
        KVHashRange hash_range, EpollSelectDispatcher epoll,
        int backlog )
    {
        this.hash_range = hash_range;

        auto conn_setup_params = new KVConnectionSetupParams;
        conn_setup_params.node_info = this;
        conn_setup_params.epoll = epoll;
        conn_setup_params.storage_channels = channels;
        conn_setup_params.shared_resources = new SharedResources;

        super(node_item, channels, conn_setup_params, backlog);

        debug
        {
            Stderr.format("Supported commands: ");
            foreach ( i, desc, command; DhtConst.Command() )
            {
                auto dht_cmd = cast(DhtConst.Command.E)command;
                if ( command != DhtConst.Command.E.None )
                {
                    Stderr.format("{}:{}", desc,
                        channels.commandSupported(dht_cmd));
                    if ( i < DhtConst.Command().length - 1 )
                    {
                        Stderr.format(", ");
                    }
                }
            }

            Stderr.formatln("\nResponsible for hash range 0x{:X} - 0x{:X}",
                min_hash, max_hash);
        }
    }


    /***************************************************************************

        Returns:
            Minimum hash supported by key/value node.

    ***************************************************************************/

    public hash_t min_hash ( )
    {
        return this.hash_range.range.min;
    }


    /***************************************************************************

        Returns:
            Maximum hash supported by key/value node.

    ***************************************************************************/

    public hash_t max_hash ( )
    {
        return this.hash_range.range.max;
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

