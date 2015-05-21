/*******************************************************************************

    Push request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release
                    August 2011: Fiber-based version

    authors:        Gavin Norman

*******************************************************************************/

module dmqnode.request.PushRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import dmqnode.storage.model.StorageEngine;
private import dmqnode.request.model.IDmqRequestResources;

private import Protocol = dmqproto.node.request.Push;

/*******************************************************************************

    Push request

*******************************************************************************/

public scope class PushRequest : Protocol.Push
{
    /***************************************************************************

        Channel storage cache, to avoid re-fetching it from different methods

    ***************************************************************************/

    private StorageEngine storage_channel;

    /***************************************************************************

        Shared resource acquirer

    ***************************************************************************/

    private const IDmqRequestResources resources;

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

    ***************************************************************************/

    override protected void pushValue ( char[] channel_name, void[] value )
    {
        assert (this.storage_channel);
        // legacy char[] values :(
        this.storage_channel.push(cast(char[]) value);
    }
}
