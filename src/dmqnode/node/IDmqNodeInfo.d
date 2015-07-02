/*******************************************************************************

    Information interface for the Distributed Message Queue Node

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module dmqnode.node.IDmqNodeInfo;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.model.IChannelsNodeInfo;

private import dmqnode.storage.Ring;


interface IDmqNodeInfo : IChannelsNodeInfo
{
    /***************************************************************************

        Returns:
            maximum number of bytes per channel

    ***************************************************************************/

    public ulong channelSizeLimit ( );

    /***************************************************************************

        'foreach' iteration over the channels.

    ***************************************************************************/

    public int opApply ( int delegate ( ref RingNode.Ring channel ) dg );
}
