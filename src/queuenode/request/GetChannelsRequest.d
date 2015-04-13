/*******************************************************************************

    GetChannels request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release
                    August 2011: Fiber-based version

    authors:        Gavin Norman

*******************************************************************************/

module queuenode.request.GetChannelsRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import Protocol = queueproto.node.request.GetChannels;

private import queuenode.request.model.IQueueRequestResources;

private import tango.transition;


/*******************************************************************************

    GetChannels request

*******************************************************************************/

public scope class GetChannelsRequest : Protocol.GetChannels
{
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
        super(reader, writer, resources);
        this.resources = resources;
    }

    /***************************************************************************

        Performs this request. (Fiber method.)

    ***************************************************************************/

    override protected char[][] getChannelsIds ( )
    {
        auto list = *this.resources.channel_list_buffer;

        foreach (channel; this.resources.storage_channels)
            list ~= channel.id;
        return list;
    }
}
