/*******************************************************************************

    RemoveChannel request.

    copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved

*******************************************************************************/

module dmqnode.request.RemoveChannelRequest;


import dmqnode.request.model.IDmqRequestResources;

import Protocol = dmqproto.node.request.RemoveChannel;

/*******************************************************************************

    RemoveChannel request

*******************************************************************************/

public scope class RemoveChannelRequest : Protocol.RemoveChannel
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

        Removes the specified channel from the storage engine

    ***************************************************************************/

    override protected void removeChannel ( char[] channel )
    {
        this.resources.storage_channels.remove(channel);
    }
}
