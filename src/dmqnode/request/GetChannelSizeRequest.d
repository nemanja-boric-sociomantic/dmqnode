/*******************************************************************************

    GetChannelSize request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

*******************************************************************************/

module dmqnode.request.GetChannelSizeRequest;


import dmqnode.request.model.IDmqRequestResources;

import Protocol = dmqproto.node.request.GetChannelSize;

/*******************************************************************************

    GetChannelSize request

*******************************************************************************/

public scope class GetChannelSizeRequest : Protocol.GetChannelSize
{
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
    }
}
