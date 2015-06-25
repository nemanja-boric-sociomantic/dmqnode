/*******************************************************************************

    GetSizeLimit request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release
                    August 2011: Fiber-based version

    authors:        Gavin Norman

*******************************************************************************/

module dmqnode.request.GetSizeLimitRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import dmqnode.request.model.IDmqRequestResources;

private import Protocol = dmqproto.node.request.GetSizeLimit;

/*******************************************************************************

    GetSizeLimit request

*******************************************************************************/

public scope class GetSizeLimitRequest : Protocol.GetSizeLimit
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

        Get channel size limit as defined by storage_channels

        Returns:
            metadata that includes amount of established connections

    ***************************************************************************/

    override protected SizeLimitData getSizeLimitData ( )
    {
        SizeLimitData data;
        data.address = this.resources.node_info.node_item.Address;
        data.port = this.resources.node_info.node_item.Port;
        data.limit = this.resources.storage_channels.channelSizeLimit;
        return data;
    }
}

