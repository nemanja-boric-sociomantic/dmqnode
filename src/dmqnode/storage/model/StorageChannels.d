/*******************************************************************************

    Queue Storage Channels manager interface

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        March 2010: Initial release

    authors:        David Eckardt, Gavin Norman

    Extends the core storage channels base class with the following features:
        * A method to get the number of bytes a record will take when stored in
          the storage engine (including any required headers, etc).
        * A per-channel size limit, in addition to the global node size limit.

*******************************************************************************/

module dmqnode.storage.model.StorageChannels;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.storage.model.IStorageChannels;

private import dmqnode.storage.model.StorageEngine;



/*******************************************************************************

    StorageChannels base class

*******************************************************************************/

public abstract class StorageChannels :
    IStorageChannelsTemplate!(StorageEngine)
{
    /***************************************************************************

        Per-channel size limit (0 = no per-channel size limit)

    ***************************************************************************/

    private ulong channel_size_limit;


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
            size_limit = maximum number of bytes allowed in the node (0 = no
                limit)
            channel_size_limit = maximum number of bytes allowed per channel (0
                = no limit)

    ***************************************************************************/

    public this ( ulong size_limit = 0, ulong channel_size_limit = 0 )
    {
        super(size_limit);

        this.channel_size_limit = channel_size_limit;
    }


    /***************************************************************************

        Returns:
            per channel size limit in bytes (0 = no size limit)

    ***************************************************************************/

    public ulong channelSizeLimit ( )
    {
        return this.channel_size_limit;
    }

    /***************************************************************************

        Writes disk overflow index.

    ***************************************************************************/

    abstract public void writeDiskOverflowIndex ( );
}

