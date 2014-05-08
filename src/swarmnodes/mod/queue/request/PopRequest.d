/*******************************************************************************

    Pop request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release
                    August 2011: Fiber-based version

    authors:        Gavin Norman

*******************************************************************************/

module swarmnodes.mod.queue.request.PopRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.mod.queue.request.model.IChannelRequest;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Pop request

*******************************************************************************/

public scope class PopRequest : IChannelRequest
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

    protected void readRequestData_ ( )
    {
    }


    /***************************************************************************

        Performs this request. (Fiber method.)

    ***************************************************************************/

    protected void handle__ ( )
    {
        this.writer.write(QueueConst.Status.E.Ok);

        auto storage_channel =
            *this.resources.channel_buffer in this.resources.storage_channels;
        if ( storage_channel !is null )
        {
            storage_channel.pop(*this.resources.value_buffer);
        }
        else
        {
            (*this.resources.value_buffer).length = 0;
        }

        this.writer.writeArray(*this.resources.value_buffer);

        this.resources.node_info.handledRecord();
    }
}

