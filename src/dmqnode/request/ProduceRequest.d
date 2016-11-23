/*******************************************************************************

    Produce request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release
                    August 2011: Fiber-based version

    authors:        Gavin Norman

*******************************************************************************/

module dmqnode.request.ProduceRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import dmqnode.request.model.IDmqRequestResources;
private import dmqnode.storage.model.StorageEngine;
private import Protocol = dmqproto.node.request.Produce;

private import swarm.core.common.request.helper.LoopCeder;



/*******************************************************************************

    Produce request

*******************************************************************************/

public scope class ProduceRequest : Protocol.Produce
{
    /***************************************************************************

        Set upon starting valid Produce request, reused when pushing records
        for that request (so that it won't be fetched for each record)

    ***************************************************************************/

    private StorageEngine storage_channel;

    /***************************************************************************

        Shared resource acquirer

    ***************************************************************************/

    private IDmqRequestResources resources;

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
        this.storage_channel = null;
    }

    /***************************************************************************

        Ensures that requested channel exists or can be created

        Params:
            channel_name = name of channel to be prepared

        Return:
            `true` if it is possible to proceed with Produce request

    ***************************************************************************/

    override protected bool prepareChannel ( char[] channel_name )
    {
        this.storage_channel = this.resources.storage_channels.getCreate(
            *this.resources.channel_buffer);
        return this.storage_channel !is null;
    }

    /***************************************************************************

        Pushes a received record to the queue.

        Params:
            channel_name = name of channel to push to
            value = record value to push

    ***************************************************************************/

    override protected void pushRecord ( char[] channel_name, char[] value )
    {
        assert (this.storage_channel !is null);
        this.storage_channel.push(value);
        this.resources.loop_ceder.handleCeding();
    }
}
