/*******************************************************************************

    Consume request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release
                    August 2011: Fiber-based version

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.queue.request.ConsumeRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.queue.request.model.IChannelRequest;

private import src.mod.queue.storage.model.QueueStorageEngine;

private import ocean.core.Array : copy;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Consume request

*******************************************************************************/

public scope class ConsumeRequest : IChannelRequest, QueueStorageEngine.IConsumer
{
    /***************************************************************************

        Storage channel being read from. The reference is only set once the
        request begins processing.

    ***************************************************************************/

    private QueueStorageEngine storage_channel;


    /***************************************************************************

        Set to true when the handle___() method is waiting for the fiber select
        event to be triggered.

    ***************************************************************************/

    private bool waiting_for_trigger;


    /***************************************************************************

        Set to true when a buffer flush is requested.

    ***************************************************************************/

    private bool flush_trigger;


    /***************************************************************************

        Set to true when the request must end (in the case where the channel
        being listened to is deleted).

    ***************************************************************************/

    private bool finish_trigger;


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

        IListener interface method. Called to trigger an event, e.g. when data
        is ready to be consumed.

        Params:
            code = trigger event code

        In:
            code must not be code.None.

    ***************************************************************************/

    public void trigger ( Code code )
    {
        with ( Code ) switch ( code )
        {
            case DataReady:
                // No action necessary, as the handle__() method always tries to
                // read from the storage engine every time around the loop.
                break;

            case Flush:
                this.flush_trigger = true;
                break;

            case Finish:
                this.finish_trigger = true;
                break;
        }

        if ( this.waiting_for_trigger )
        {
            this.resources.event.trigger;
        }
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

        TODO: upon failure (presumably in the case where a broken pipe exception
        is thrown), we now potentially have a write buffer full of queue records
        which are ready to be sent. At the moment we just discard them. A super
        friendly queue node might push them back into the appropriate channel.

    ***************************************************************************/

    protected void handle__ ( )
    {
        this.storage_channel = this.resources.storage_channels.getCreate(
            *this.resources.channel_buffer);
        if ( this.storage_channel is null )
        {
            this.writer.write(QueueConst.Status.E.Error);
            return;
        }

        this.storage_channel.registerConsumer(this);
        scope ( exit )
        {
            this.storage_channel.unregisterConsumer(this);
        }

        this.writer.write(QueueConst.Status.E.Ok);

        // Note: the Finish code is only received when the storage channel being
        // consumed is recycled.
        while ( !this.finish_trigger )
        {
            // Consume any records which are ready
            while ( this.storage_channel.num_records > 0 )
            {
                this.storage_channel.pop(*this.resources.value_buffer);

                this.writer.writeArray(*this.resources.value_buffer);

                this.resources.node_info.handledRecord();

                this.resources.loop_ceder.handleCeding();
            }

            // Wait until a trigger occurs (may exit immediately if a trigger
            // has already happened)
            this.waitForTrigger();

            // Handle flushing
            if ( this.flush_trigger || this.finish_trigger )
            {
                this.writer.flush();
                this.flush_trigger = false;
            }
        }

        // Write empty value, informing the client that the request has
        // finished
        this.writer.writeArray("");
    }


    /***************************************************************************

        Waits for the select event to fire (see the trigger() method, above).
        Will return immediately without waiting if a flush or finish trigger has
        already happened.

    ***************************************************************************/

    private void waitForTrigger ( )
    {
        if ( this.finish_trigger || this.flush_trigger )
        {
            return;
        }

        this.waiting_for_trigger = true;
        scope ( exit ) this.waiting_for_trigger = false;

        this.resources.event.wait();
    }
}

