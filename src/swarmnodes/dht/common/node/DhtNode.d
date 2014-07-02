/*******************************************************************************

    Distributed Hashtable Node Implementation

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2009: Initial release
                    December 2010: Asynchronous version
                    August 2011: Fibers version

    authors:        Lars Kirchhoff, Thomas Nicolai, David Eckardt, Gavin Norman

    DHT node class derived from the node base classes in swarm.

*******************************************************************************/

module swarmnodes.dht.common.node.DhtNode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.model.ChannelsNode : ChannelsNodeBase;

private import swarm.dht.DhtConst;

private import swarmnodes.dht.common.node.IDhtNodeInfo;
private import swarmnodes.dht.common.node.DhtHashRange;

private import swarmnodes.dht.common.connection.DhtConnectionHandler;

private import swarmnodes.dht.common.connection.SharedResources;

private import swarmnodes.dht.common.storage.DhtStorageEngine;
private import swarmnodes.dht.common.storage.DhtStorageChannels;

private import ocean.io.select.EpollSelectDispatcher;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    DhtNode

*******************************************************************************/

public class DhtNode : ChannelsNodeBase!(DhtStorageEngine, DhtConnectionHandler),
    IDhtNodeInfo
{
    /**************************************************************************

        Node minimum & maximum hash

    ***************************************************************************/

    private const DhtHashRange hash_range;


    /***************************************************************************

        Constructor.

        Params:
            node_item = node address/port
            channels = storage channels instance to use
            hash_range = min/max hash range tracker
            epoll = epoll select dispatcher to be used internally
            backlog = (see ISelectListener ctor)

    ***************************************************************************/

    public this ( DhtConst.NodeItem node_item, DhtStorageChannels channels,
        DhtHashRange hash_range, EpollSelectDispatcher epoll,
        int backlog )
    {
        this.hash_range = hash_range;

        auto conn_setup_params = new DhtConnectionSetupParams;
        conn_setup_params.node_info = this;
        conn_setup_params.epoll = epoll;
        conn_setup_params.storage_channels = channels;
        conn_setup_params.shared_resources = new SharedResources;

        super(node_item, channels, conn_setup_params, backlog);

        debug
        {
            Trace.format("Supported commands: ");
            foreach ( i, desc, command; DhtConst.Command() )
            {
                auto dht_cmd = cast(DhtConst.Command.E)command;
                if ( command != DhtConst.Command.E.None )
                {
                    Trace.format("{}:{}", desc,
                        channels.commandSupported(dht_cmd));
                    if ( i < DhtConst.Command().length - 1 )
                    {
                        Trace.format(", ");
                    }
                }
            }

            Trace.formatln("\nResponsible for hash range 0x{:X} - 0x{:X}",
                min_hash, max_hash);
        }
    }


    /***************************************************************************

        Returns:
            Minimum hash supported by dht node.

    ***************************************************************************/

    public hash_t min_hash ( )
    {
        return this.hash_range.range.min;
    }


    /***************************************************************************

        Returns:
            Maximum hash supported by dht node.

    ***************************************************************************/

    public hash_t max_hash ( )
    {
        return this.hash_range.range.max;
    }


    /***************************************************************************

        Returns:
            identifier string for this node

    ***************************************************************************/

    debug protected char[] id ( )
    {
        return typeof(this).stringof;
    }
}

