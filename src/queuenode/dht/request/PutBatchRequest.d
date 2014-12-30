/*******************************************************************************

    Copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

    PutBatch request handler

*******************************************************************************/

module queuenode.dht.request.PutBatchRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import queuenode.common.kvstore.request.model.IChannelRequest;
private import queuenode.common.kvstore.request.model.IPutSingleRequest;



public scope class PutBatchRequest : IChannelRequest
{
    /***************************************************************************

        Constructor

        Params:
            cmd = command code
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader,
        FiberSelectWriter writer, IKVRequestResources resources )
    {
        super(DhtConst.Command.E.PutBatch, reader, writer, resources);
    }


    /***************************************************************************

        Reads any data from the client which is required for the request. If the
        request is invalid in some way (the channel name is invalid, or the
        command is not supported) then the command can be simply not executed,
        and all client data has been read, leaving the read buffer in a clean
        state ready for the next request.

    ***************************************************************************/

    override protected void readRequestData_ ( )
    {
        super.reader.readArray(*this.resources.batch_buffer);
    }


    /***************************************************************************

        Performs this request. (Fiber method, after command and channel validity
        have been confirmed.)

    ***************************************************************************/

    override protected void handle___ ( )
    {
        auto status = DhtConst.Status.E.Ok;

        this.resources.record_batch.decompress(
            cast(ubyte[])*this.resources.batch_buffer);

        foreach ( key, value; this.resources.record_batch )
        {
            this.resources.node_info.handledRecord();

            bool error;
            status = IPutSingleRequest.put(this.resources.storage_channels,
                *this.resources.channel_buffer, key, value,
                ( KVStorageEngine channel, char[] key, char[] value )
                {
                    channel.put(key, value);
                }
            );
            if ( status != DhtConst.Status.E.Ok ) break;
        }

        this.writer.write(status);
    }
}

