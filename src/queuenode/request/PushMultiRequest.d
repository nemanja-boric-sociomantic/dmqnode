/*******************************************************************************

    PushMulti request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release
                    August 2011: Fiber-based version

    authors:        Gavin Norman

*******************************************************************************/

module queuenode.request.PushMultiRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import queuenode.request.model.IMultiChannelRequest;

private import swarm.core.Const;



/*******************************************************************************

    PushMulti request

*******************************************************************************/

public scope class PushMultiRequest : IMultiChannelRequest
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

        Notes:
            * The push will be performed on all channels with space, however if
              any channel does not have space then the OutOfMemory code is
              returned to the client.
            * The push will not be performed if any of the received channel
              names is invalid.

    ***************************************************************************/

    protected void handle_ ( )
    {
        // Check whether all channel names are ok, and whether the value will
        // fit in the specified channels
        QueueConst.Status.E status;
        auto push = this.decideStatus(status);

        // Write status
        this.writer.write(status);

        // Push to channels
        if ( push )
        {
            foreach ( i, channel; this.channels )
            {
                if ( this.resources.channel_flags_buffer[i] )
                {
                    this.resources.storage_channels.getCreate(channel)
                        .push(*this.resources.value_buffer);
                }
            }
        }
    }


    /***************************************************************************

        Decides which status code to return to the client, and whether the push
        request should be performed. The default Ok status is only sent if all
        received channel names are valid, and if the received record will fit in
        all channels.

        This method updates the channel flags array, indicating whether each
        channel in this.channels should be pushed to or not.

        Otherwise:
            * If the received value is empty then the EmptyValue code is
              returned to the client and nothing is pushed.
            * If any of the received channel names is invalid  then the
              BadChannelName code is returned and nothing is pushed.
            * If any channel does not have space then the OutOfMemory code is
              returned to the client and the value is pushed to any channels in
              which it will fit.

        Params:
            status = receives status to return to client

        Returns:
            true if the push should be performed (if all channel names are
            valid, all channels can be created, and the record will fit in at
            least one channel)

    ***************************************************************************/

    private bool decideStatus ( out QueueConst.Status.E status )
    {
        (*this.resources.channel_flags_buffer).length = this.channels.length;
        (*this.resources.channel_flags_buffer)[] = false;

        // Check for empty value
        if ( (*this.resources.value_buffer).length == 0 )
        {
            status = QueueConst.Status.E.EmptyValue;
            return false;
        }

        // Check whether value would exceed global size limit
        if ( !this.resources.storage_channels.sizeLimitOk(
            (*this.resources.value_buffer).length) )
        {
            status = QueueConst.Status.E.OutOfMemory;
            return false;
        }

        uint full_channels;

        foreach ( i, channel; this.channels )
        {
            // Check if channel name is valid
            if ( !validateChannelName(channel) )
            {
                status = QueueConst.Status.E.BadChannelName;
                return false;
            }

            // Check that channel exists or can be created
            auto storage_channel = this.resources.storage_channels.getCreate(
                channel);
            if ( storage_channel is null )
            {
                status = QueueConst.Status.E.Error;
                return false;
            }

            // Check whether value will fit in this channel
            if ( this.resources.storage_channels.sizeLimitOk(channel,
                (*this.resources.value_buffer).length) &&
                storage_channel.willFit(*this.resources.value_buffer) )
            {
                (*this.resources.channel_flags_buffer)[i] = true;
            }
            else
            {
                full_channels++;
            }
        }

        status = full_channels == 0
                ? QueueConst.Status.E.Ok
                : QueueConst.Status.E.OutOfMemory;

        return full_channels < this.channels.length;
    }
}

