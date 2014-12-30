/*******************************************************************************

    Get request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module queuenode.dht.request.GetRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import queuenode.common.kvstore.request.model.ISingleKeyRequest;



/*******************************************************************************

    Get request

*******************************************************************************/

public scope class GetRequest : ISingleKeyRequest
{
    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        IKVRequestResources resources )
    {
        super(DhtConst.Command.E.Get, reader, writer, resources);
    }


    /***************************************************************************

        Performs this request. (Fiber method, after command and channel validity
        have been confirmed.)

    ***************************************************************************/

    protected void handle___ ( )
    {
        this.writer.write(DhtConst.Status.E.Ok);

        auto storage_channel =
            *this.resources.channel_buffer in this.resources.storage_channels;
        if ( storage_channel !is null )
        {
            storage_channel.get(*this.resources.key_buffer,
                *this.resources.value_buffer);
        }

        this.writer.writeArray(*this.resources.value_buffer);

        this.resources.node_info.handledRecord();
    }
}

