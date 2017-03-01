/*******************************************************************************

    Queue Storage Channels manager interface

    Extends the core storage channels base class with the following features:
        * A method to get the number of bytes a record will take when stored in
          the storage engine (including any required headers, etc).
        * A per-channel size limit, in addition to the global node size limit.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

*******************************************************************************/

module dmqnode.storage.model.StorageChannels;


import dmqnode.storage.model.StorageEngine;

import swarm.core.node.storage.model.IStorageChannels;

/*******************************************************************************

    StorageChannels base class

*******************************************************************************/

public abstract class StorageChannels :
    IStorageChannelsTemplate!(StorageEngine)
{
    /***************************************************************************

        Calculates the size (in bytes) an item would take if it were pushed
        to the queue.

        Params:
            len = length of data item

        Returns:
            bytes that data will claim in the queue

    ***************************************************************************/

    abstract protected size_t pushSize ( size_t additional_size );


    /***************************************************************************

        Constructor

        Params:
            size_limit = maximum number of bytes allowed in the node

    ***************************************************************************/

    public this ( ulong size_limit )
    {
        super(size_limit);
    }


    /***************************************************************************

        Returns:
            the default size limit per channel in bytes.

    ***************************************************************************/

    abstract public ulong channelSizeLimit ( );

    /***************************************************************************

        Writes disk overflow index.

    ***************************************************************************/

    abstract public void writeDiskOverflowIndex ( );
}
