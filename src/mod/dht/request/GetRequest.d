/*******************************************************************************

    Get request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.dht.request.GetRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.dht.request.model.IChannelRequest;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Get request

*******************************************************************************/

public scope class GetRequest : IChannelRequest
{
    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        IDhtRequestResources resources )
    {
        super(DhtConst.Command.E.Get, reader, writer, resources);
    }


    /***************************************************************************

        Reads any data from the client which is required for the request. If the
        request is invalid in some way (the channel name is invalid, or the
        command is not supported) then the command can be simply not executed,
        and all client data has been read, leaving the read buffer in a clean
        state ready for the next request.

    ***************************************************************************/

    protected void readRequestData_ ( )
    {
        this.reader.readArray(*this.resources.key_buffer);
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

