/*******************************************************************************

    Remove request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module swarmnodes.dht.memory.request.RemoveRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.dht.common.request.model.ISingleKeyRequest;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Remove request

*******************************************************************************/

public scope class RemoveRequest : ISingleKeyRequest
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
        super(DhtConst.Command.E.Remove, reader, writer, resources);
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
            storage_channel.remove(*this.resources.key_buffer);
        }
    }
}

