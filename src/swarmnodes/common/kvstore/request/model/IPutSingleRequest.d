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
        DhtConst.Status.E status;

        if ( (*this.resources.value_buffer).length == 0 )
        {
            status = DhtConst.Status.E.EmptyValue;
        }
        else if ( !this.resources.storage_channels.responsibleForKey(
            *this.resources.key_buffer) )
        {
            status = DhtConst.Status.E.WrongNode;
        }
        else
        {
            if ( this.resources.storage_channels.sizeLimitOk(
                (*this.resources.value_buffer).length) )
            {
                auto storage_channel = this.resources.storage_channels.getCreate(
                    *this.resources.channel_buffer);
                if ( storage_channel is null )
                {
                    status = DhtConst.Status.E.Error;
                }
                else
                {
                    status = DhtConst.Status.E.Ok;
                    this.performRequest(storage_channel,
                        *this.resources.key_buffer, *this.resources.value_buffer);
                }
            }
            else
            {
                status = DhtConst.Status.E.OutOfMemory;
            }
        }

        this.writer.write(status);
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

