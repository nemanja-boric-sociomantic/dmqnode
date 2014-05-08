/*******************************************************************************

    Push request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release
                    August 2011: Fiber-based version

    authors:        Gavin Norman

*******************************************************************************/

module swarmnodes.queue.request.PushRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.queue.request.model.IChannelRequest;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Push request

*******************************************************************************/

public scope class PushRequest : IChannelRequest
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
        this.reader.readArray(*this.resources.value_buffer);

        this.resources.node_info.handledRecord();
    }


    /***************************************************************************

        Performs this request. (Fiber method.)

    ***************************************************************************/

    protected void handle__ ( )
    {
        QueueConst.Status.E status;

        if ( (*this.resources.value_buffer).length )
        {
            if ( this.resources.storage_channels.sizeLimitOk(
                *this.resources.channel_buffer,
                (*this.resources.value_buffer).length) )
            {
                auto storage_channel =
                    this.resources.storage_channels.getCreate(
                    *this.resources.channel_buffer);
                if ( storage_channel is null )
                {
                    status = QueueConst.Status.E.Error;
                }
                else
                {
                    status = storage_channel.push(*this.resources.value_buffer)
                           ? QueueConst.Status.E.Ok
                           : QueueConst.Status.E.OutOfMemory;
                }
            }
            else
            {
                status = QueueConst.Status.E.OutOfMemory;
            }
        }
        else
        {
            status = QueueConst.Status.E.EmptyValue;
        }

        this.writer.write(status);
    }
}

