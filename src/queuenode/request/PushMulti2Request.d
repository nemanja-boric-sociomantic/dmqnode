/*******************************************************************************

    PushMulti2 request class.

    copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

*******************************************************************************/

module queuenode.request.PushMulti2Request;



/*******************************************************************************

    Imports

*******************************************************************************/

private import queuenode.request.model.IMultiChannelRequest;

private import swarm.core.Const;



/*******************************************************************************

    PushMulti2 request

*******************************************************************************/

public scope class PushMulti2Request : IMultiChannelRequest
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
        super(QueueConst.Command.E.PushMulti, reader, writer, resources);
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
        // Read value
        this.reader.readArray(*this.resources.value_buffer);
    }


    /***************************************************************************

        Performs this request. (Fiber method.)

        The status code which is sent to the client and the behaviour of this
        request handler are determined as follows:
            * If the received value is empty, then the EmptyValue code is
              returned to the client and nothing is pushed.
            * If pushing the received record into the specified number of
              channels would exceed the global size limit, then the OutOfMemory
              code is returned to the client and nothing is pushed.
            * If any of the received channel names is invalid, then the
              BadChannelName code is returned and nothing is pushed.
            * If any of the specified channels does not exist or cannot be
              created, then the Error code is returned to the client and nothing
              is pushed.
            * Otherwise, the Ok code is returned to the client, the record is
              pushed to each channel, and the names of any channels to which the
              record could not be pushed are sent to the client, as described
              below.

        The pushing behaviour (occurring when the Ok status is returned to the
        client) is as follows:
            * For each channel specified, if the received record fits in the
              space available, then it is pushed.
            * If the received record does not fit in the space available in a
              channel, then the channel's name is sent to the client.
            * When all of the specified channels have been handled, an end-of-
              list terminator (an empty string) is sent to the client.

        Thus, in the case when the queuenode is able to push the received record
        into all of the specified channels, the client will receive the Ok
        status followed by an empty string (indicating an empty list of failed
        channels).

    ***************************************************************************/

    protected void handle_ ( )
    {
        // Check whether all channel names are ok, and whether the value will
        // fit in the specified channels
        auto status = this.decideStatus();

        // Write status and exit on error
        this.writer.write(status);
        if ( status != status.Ok ) return;

        // Push to channels in which the value will fit, and send the client the
        // names of any channels not pushed to
        foreach ( i, channel; this.channels )
        {
            auto storage_channel = this.resources.storage_channels.getCreate(
                channel);
            assert(storage_channel !is null, "storage channel accessor succeeded "
                "on first call but failed on second");

            if ( this.resources.storage_channels.sizeLimitOk(channel,
                (*this.resources.value_buffer).length) &&
                storage_channel.willFit(*this.resources.value_buffer) )
            {
                this.resources.storage_channels.getCreate(channel)
                    .push(*this.resources.value_buffer);
            }
            else
            {
                this.writer.writeArray(channel);
            }
        }

        // Terminate list of failed channels with an empty string
        this.writer.writeArray("");
    }


    /***************************************************************************

        Decides which status code to return to the client, as follows:
            * If the received value is empty, then the EmptyValue code is
              returned.
            * If pushing the received record into the specified number of
              channels would exceed the global size limit, then the OutOfMemory
              code is returned.
            * If any of the received channel names is invalid, then the
              BadChannelName code is returned.
            * If any of the specified channels does not exist or cannot be
              created, then the Error code is returned.
            * Otherwise, the Ok code is returned.

        Returns:
            status code to be sent to client

    ***************************************************************************/

    private QueueConst.Status.E decideStatus ( )
    {
        // Check for empty value
        if ( (*this.resources.value_buffer).length == 0 )
        {
            return QueueConst.Status.E.EmptyValue;
        }

        // Check whether pushing value into specified number of channels would
        // exceed the global size limit
        if ( !this.resources.storage_channels.sizeLimitOk(
            (*this.resources.value_buffer).length * this.channels.length) )
        {
            return QueueConst.Status.E.OutOfMemory;
        }

        foreach ( i, channel; this.channels )
        {
            // Check if channel name is valid
            if ( !validateChannelName(channel) )
            {
                return QueueConst.Status.E.BadChannelName;
            }

            // Check that channel exists or can be created
            auto storage_channel = this.resources.storage_channels.getCreate(
                channel);
            if ( storage_channel is null )
            {
                return QueueConst.Status.E.Error;
            }
        }

        return QueueConst.Status.E.Ok;
    }
}

