/*******************************************************************************

    GetChannelSize request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release
                    August 2011: Fiber-based version

    authors:        Gavin Norman

*******************************************************************************/

module dmqnode.request.GetChannelSizeRequest;


/*******************************************************************************

    Imports

*******************************************************************************/

private import Protocol = dmqproto.node.request.GetChannelSize;

private import dmqnode.request.model.IDmqRequestResources;


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

