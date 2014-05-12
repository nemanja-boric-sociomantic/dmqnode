/*******************************************************************************

    Get channels request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module swarmnodes.dht.common.request.GetChannelsRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.dht.common.request.model.IRequest;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Get channels request

*******************************************************************************/

public scope class GetChannelsRequest : IRequest
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
        super(DhtConst.Command.E.GetChannels, reader, writer, resources);
    }


    /***************************************************************************

        Reads any data from the client which is required for the request. If the
        request is invalid in some way (the channel name is invalid, or the
        command is not supported) then the command can be simply not executed,
        and all client data has been read, leaving the read buffer in a clean
        state ready for the next request.

    ***************************************************************************/

    protected void readRequestData ( )
    {
    }


    /***************************************************************************

        Performs this request. (Fiber method.)

    ***************************************************************************/

    protected void handle__ ( )
    {
        this.writer.write(DhtConst.Status.E.Ok);

        foreach ( channel; this.resources.storage_channels )
        {
            this.writer.writeArray(channel.id);
        }

        this.writer.writeArray(""); // End of list
    }
}

