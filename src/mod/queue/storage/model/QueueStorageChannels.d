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

module src.mod.queue.storage.model.QueueStorageChannels;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.storage.model.IStorageChannels;

private import src.mod.queue.storage.model.QueueStorageEngine;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    QueueStorageChannels base class

*******************************************************************************/

public abstract class QueueStorageChannels :
    IStorageChannelsTemplate!(QueueStorageEngine)
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

        Note: overriding super class method to work around compiler bug which
        makes either the super or derived version of this method available.

        Tells whether the size of all records in the storage channels, plus the
        optional extra size specified, exceed the defined size limit.

        Note: this method only checks the size of the bytes in the storage
        channel(s), it *does not* guarantee that the storage engine will
        successfully be able to push the additional data -- the only way is to
        do the push and check the push method's return value.

        Params:
            additional_size = additional data size to test whether it'd fit

        Returns:
            true if size of all records (plus additional size) is less than the
            defined size limit for the whole node

    ***************************************************************************/

    public override bool sizeLimitOk ( size_t additional_size )
    {
        return super.sizeLimitOk(additional_size);
    }


    /***************************************************************************

        Tells whether the size of all records in the storage channels, plus the
        optional extra size specified, exceed the defined size limit, and
        whether the size of all records in the specified channel, plus the
        optional extra size specified, exceed the defined per channel size
        limit.

        Note: this method only checks the size of the bytes in the storage
        channel(s), it *does not* guarantee that the storage engine will
        successfully be able to push the additional data -- the only way is to
        do the push and check the push method's return value.

        Params:
            channel = channel data would be added to
            additional_size = additional data size to test whether it'd fit

        Returns:
            true if size of all records (plus additional size) is less than the
            defined size limit for the whole node and per channel

    ***************************************************************************/

    public bool sizeLimitOk ( char[] channel, size_t additional_size )
    {
        auto push_size = this.pushSize(additional_size);

        // Check the global size limit (all channels combined), then check the
        // per-channel size limit
        if ( this.sizeLimitOk(additional_size) )
        {
            if ( this.channel_size_limit == 0 ) return true;

            ulong channel_size;
            auto storage_channel = channel in super;
            if ( storage_channel !is null )
            {
                channel_size = storage_channel.num_bytes;
            }

            return channel_size + push_size <= this.channel_size_limit;
        }
        else
        {
            return false;
        }
    }
}

