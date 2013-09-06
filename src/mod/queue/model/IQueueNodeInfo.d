/*******************************************************************************

    Information interface for Queue Node

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/  

module src.mod.queue.model.IQueueNodeInfo;



/*******************************************************************************

    Imports

*******************************************************************************/  

private import swarm.core.node.model.IChannelsNodeInfo;



interface IQueueNodeInfo : IChannelsNodeInfo
{
    /***************************************************************************

        Returns:
            maximum number of bytes per channel

    ***************************************************************************/

    public ulong channelSizeLimit ( );
}

