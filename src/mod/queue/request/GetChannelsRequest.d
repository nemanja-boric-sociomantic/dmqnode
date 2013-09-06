/*******************************************************************************

    GetChannels request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release
                    August 2011: Fiber-based version

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.queue.request.GetChannelsRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.queue.request.model.IRequest;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    GetChannels request

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
        IQueueRequestResources resources )
    {
        super(reader, writer, resources);
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

    protected void handle_ ( )
    {
        this.writer.write(QueueConst.Status.E.Ok);

        foreach ( channel; this.resources.storage_channels )
        {
            this.writer.writeArray(channel.id);
        }

        this.writer.writeArray(""); // End of list
    }
}

