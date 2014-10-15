/*******************************************************************************

    Information interface for Distributed Hashtable Node

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        February 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module swarmnodes.common.kvstore.node.IKVNodeInfo;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.model.IChannelsNodeInfo;



public interface IKVNodeInfo : IChannelsNodeInfo
{
    /***************************************************************************

        Returns:
            Minimum hash supported by key/value node.

    ***************************************************************************/

    public hash_t min_hash ( );


    /***************************************************************************

        Returns:
            Maximum hash supported by key/value node.

    ***************************************************************************/

    public hash_t max_hash ( );
}

