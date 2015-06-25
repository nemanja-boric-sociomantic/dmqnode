/*******************************************************************************

    Consume request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release
                    August 2011: Fiber-based version

    authors:        Gavin Norman

*******************************************************************************/

module queuenode.request.ConsumeRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import queuenode.storage.model.StorageEngine;
private import queuenode.request.model.IDmqRequestResources;
private import Protocol = dmqproto.node.request.Consume;

private import ocean.core.Array : copy;



/*******************************************************************************

    Consume request

*******************************************************************************/

public scope class ConsumeRequest : Protocol.Consume, StorageEngine.IConsumer
{
    /***************************************************************************

        Storage channel being read from. The reference is only set once the
        request begins processing.

    ***************************************************************************/

    private StorageEngine storage_channel;

    /***************************************************************************

        Shared resource acquirer

    ***************************************************************************/

    private const IDmqRequestResources resources;

    /***************************************************************************

        Set to true when the handle___() method is waiting for the fiber select
        event to be triggered.

    ***************************************************************************/

    private bool waiting_for_trigger;

    /***************************************************************************

        Flags used to communicate event outcomes from even callback to other
        class methods.

    ***************************************************************************/

    private bool finish_trigger, flush_trigger;

    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        IDmqRequestResources resources )
    {
        super(reader, writer, resources);
        this.resources = resources;
    }

    /***************************************************************************

        Ensures that requested channel exists and can be read from. 

        Params:
            channel_name = name of channel to be prepared

        Return:
            `true` if it is possible to proceed with Consume request

    ***************************************************************************/

    override protected bool prepareChannel ( char[] channel_name )
    {
        this.storage_channel = this.resources.storage_channels.getCreate(
            channel_name);

        if (this.storage_channel is null)
            return false;

        // unregistered in this.finalizeRequest
        this.storage_channel.registerConsumer(this);

        return true;
    }

    /***************************************************************************

        Retrieve next value from the channel if available

        Params:
            channel_name = channel to get value from
            value        = array to write value to

        Returns:
            `true` if there was a value in the channel

    ***************************************************************************/

    override protected bool getNextValue ( char[] channel_name, ref char[] value )
    {
        // Consume any records which are ready
        if (this.storage_channel.num_records > 0)
        {
            this.storage_channel.pop(value);
            this.resources.loop_ceder.handleCeding();
            return true;
        }
        else
        {
            return false;
        }
    }

    /***************************************************************************

        When there are no more elements in the channel this method allows to
        wait for more to appear or force early termination of the request.

        This method is explicitly designed to do a fiber context switch

        Params:
            finish = set to true if request needs to be ended
            flush =  set to true if socket needs to be flushed

    ***************************************************************************/

    override protected void waitEvents ( out bool finish, out bool flush )
    {
        scope(exit)
        {
            finish = this.finish_trigger;
            flush  = this.flush_trigger;
            this.finish_trigger = false;
            this.flush_trigger = false;
        }

        // have already recevied some event by that point
        if ( this.finish_trigger || this.flush_trigger )
            return;

        this.waiting_for_trigger = true;
        scope(exit) this.waiting_for_trigger = false;

        this.resources.event.wait();
    }

    /***************************************************************************

        Called upon termination of the request, any cleanup steps can be put
        here.

    ***************************************************************************/

    override protected void finalizeRequest ( )
    {
        this.storage_channel.unregisterConsumer(this);
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
}

