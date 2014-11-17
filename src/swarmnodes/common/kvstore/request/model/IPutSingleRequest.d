/*******************************************************************************

    Single value Put request base class. Used by Put & PutDup.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        August 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module swarmnodes.common.kvstore.request.model.IPutSingleRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.common.kvstore.request.model.ISingleKeyRequest;



/*******************************************************************************

    Put single request

*******************************************************************************/

public scope class IPutSingleRequest : ISingleKeyRequest
{
    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( DhtConst.Command.E cmd, FiberSelectReader reader,
        FiberSelectWriter writer, IKVRequestResources resources )
    {
        super(cmd, reader, writer, resources);
    }


    /***************************************************************************

        Reads any data from the client which is required for the request. If the
        request is invalid in some way (the channel name is invalid, or the
        command is not supported) then the command can be simply not executed,
        and all client data has been read, leaving the read buffer in a clean
        state ready for the next request.

    ***************************************************************************/

    protected void readRequestData__ ( )
    {
        this.reader.readArray(*this.resources.value_buffer);

        this.resources.node_info.handledRecord();
    }


    /***************************************************************************

        Performs this request. (Fiber method, after command and channel validity
        have been confirmed.)

    ***************************************************************************/

    protected void handle___ ( )
    {
        auto status = typeof(this).put(this.resources.storage_channels,
            *this.resources.channel_buffer, *this.resources.key_buffer,
            *this.resources.value_buffer, &this.performRequest);

        this.writer.write(status);
    }


    /***************************************************************************

        Does a series of checks required to validate putting of a single record.
        If all checks pass performs the specified action.

        Params:
            node = storage channels instance
            channel = name of channel to put to
            key = key to put
            value = value to put
            action = action to be performed if all checks pass

        Returns:
            status code representing the result of the checks. Ok if all
            succeeded, or a non-Ok code otherwise

    ***************************************************************************/

    static public DhtConst.Status.E put ( KVStorageChannels node,
        char[] channel, char[] key, char[] value,
        void delegate ( KVStorageEngine channel, char[] key, char[] value ) action )
    {
        if ( value.length == 0 )
        {
            return DhtConst.Status.E.EmptyValue;
        }

        if ( !node.responsibleForKey(key) )
        {
            return DhtConst.Status.E.WrongNode;
        }

        if ( !node.sizeLimitOk(value.length) )
        {
            return DhtConst.Status.E.OutOfMemory;
        }

        auto storage_channel = node.getCreate(channel);
        if ( storage_channel is null )
        {
            return DhtConst.Status.E.Error;
        }

        action(storage_channel, key, value);
        return DhtConst.Status.E.Ok;
    }


    /***************************************************************************

        Performs the put request on the storage channel request.

        Params:
            channel = channel to put to
            key = key to put
            value = value to put

    ***************************************************************************/

    abstract protected void performRequest ( KVStorageEngine channel,
        char[] key, char[] value );
}

