/*******************************************************************************

    PushMulti request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release
                    August 2011: Fiber-based version

    authors:        Gavin Norman

*******************************************************************************/

module dmqnode.request.PushMultiRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

private import dmqnode.request.model.IDmqRequestResources;

private import Protocol = dmqproto.node.request.PushMulti;

/*******************************************************************************

    PushMulti request

*******************************************************************************/

public scope class PushMultiRequest : Protocol.PushMulti
{
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

    /***************************************************************************

        Ensure that requested channels exist / can be created and can be written
        to.

        Params:
            channel_name = list of channel names to checl

        Returns:
            "true" if all requested channels are available
            "false" otherwise

    ***************************************************************************/

    override protected bool prepareChannels ( char[][] channel_names )
    {
        foreach (channel; channel_names)
        {
            if (!this.resources.storage_channels.getCreate(channel))
                return false;
        }

        return true;
    }

    /***************************************************************************

        PushMulti the value to the channel.

        Params:
            channel_name = name of channel to be writter to
            value        = value to write

        Returns:
            "true" if writing the value was possible
            "false" if there wasn't enough space

    ***************************************************************************/

    override protected void pushValue ( char[] channel_name, void[] value )
    {
        auto channel = this.resources.storage_channels.getCreate(channel_name);
        assert (channel !is null); // already verified in this.prepareChannels
        channel.push(cast(char[]) value);
    }
}
