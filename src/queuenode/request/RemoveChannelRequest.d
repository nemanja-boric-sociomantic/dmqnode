/*******************************************************************************

    RemoveChannel request.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        August 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module queuenode.request.RemoveChannelRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import queuenode.request.model.IDmqRequestResources;

private import Protocol = dmqproto.node.request.RemoveChannel;

/*******************************************************************************

    RemoveChannel request

*******************************************************************************/

public scope class RemoveChannelRequest : Protocol.RemoveChannel
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

        Removes the specified channel from the storage engine

    ***************************************************************************/

    override protected void removeChannel ( char[] channel )
    {
        this.resources.storage_channels.remove(channel);
    }
}
