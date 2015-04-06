/*******************************************************************************

    GetSize request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release
                    August 2011: Fiber-based version

    authors:        Gavin Norman

*******************************************************************************/

module queuenode.request.GetSizeRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import queuenode.request.model.IQueueRequestResources;

private import Protocol = queueproto.node.request.GetSize;

/*******************************************************************************

    GetSize request

*******************************************************************************/

public scope class GetSizeRequest : Protocol.GetSize
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
        super(reader, writer);
        this.resources = resources;
    }

    /***************************************************************************

        Calculates and return aggregated record count and total size for all
        channels

        Returns:
            metadata that includes aggregated size of all channels

    ***************************************************************************/

    override protected SizeData getSizeData ( )
    {
        SizeData data;
        data.address = this.resources.node_info.node_item.Address;
        data.port = this.resources.node_info.node_item.Port;

        foreach ( channel; this.resources.storage_channels )
        {
            data.records += channel.num_records;
            data.bytes += channel.num_bytes;
        }

        return data;
    }
}
