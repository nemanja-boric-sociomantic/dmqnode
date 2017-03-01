/*******************************************************************************

    Pop request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

*******************************************************************************/

module dmqnode.request.PopRequest;


import dmqnode.request.model.IDmqRequestResources;
import dmqnode.storage.model.StorageEngine;

import Protocol = dmqproto.node.request.Pop;

/*******************************************************************************

    Pop request

*******************************************************************************/

public scope class PopRequest : Protocol.Pop
{
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
    }

    /***************************************************************************

        Pops the last value from the channel.

        Params:
            channel_name = name of channel to be queried

        Returns:
            popped value, empty array if channel is empty

    ***************************************************************************/

    override protected void[] getNextValue ( char[] channel_name )
    {
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

        return *this.resources.value_buffer;
    }
}
