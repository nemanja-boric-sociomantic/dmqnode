/*******************************************************************************

    Push request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release
                    August 2011: Fiber-based version

    authors:        Gavin Norman

*******************************************************************************/

module queuenode.request.PushRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import queuenode.storage.model.QueueStorageEngine;
private import queuenode.request.model.IQueueRequestResources;

private import Protocol = queueproto.node.request.Push;

/*******************************************************************************

    Push request

*******************************************************************************/

public scope class PushRequest : Protocol.Push
{
    /***************************************************************************

        Channel storage cache, to avoid re-fetching it from different methods

    ***************************************************************************/

    private QueueStorageEngine storage_channel;

    /***************************************************************************

        Shared resource acquirer

    ***************************************************************************/

    private const IQueueRequestResources resources;

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
        super(reader, writer, resources.channel_buffer, resources.value_buffer);
        this.resources = resources;
    }

    /***************************************************************************

        Ensures that requested channel exists or can be created

        Params:
            channel_name = name of channel to be prepared

        Return:
            `true` if it is possible to proceed with Push request

    ***************************************************************************/

    override protected bool prepareChannel ( char[] channel_name )
    {
        this.storage_channel = this.resources.storage_channels.getCreate(
            channel_name);
        return this.storage_channel !is null;
    }

    /***************************************************************************

        Push the value to the channel.

        Params:
            channel_name = name of channel to be writter to
            value        = value to write

        Returns:
            "true" if writing the value was possible
            "false" if there wasn't enough space

    ***************************************************************************/

    override protected bool pushValue ( char[] channel_name, void[] value )
    {
        if (!this.resources.storage_channels.sizeLimitOk(channel_name,
            value.length))
        {
            return false;
        }

        assert (this.storage_channel);
        // legacy char[] values :(
        return this.storage_channel.push(cast(char[]) value);
    }
}
