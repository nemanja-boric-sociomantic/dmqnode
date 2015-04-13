/*******************************************************************************

    GetNumConnections request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release
                    August 2011: Fiber-based version

    authors:        Gavin Norman

*******************************************************************************/

module queuenode.request.GetNumConnectionsRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import queuenode.request.model.IQueueRequestResources;

private import Protocol = queueproto.node.request.GetNumConnections;

/*******************************************************************************

    GetNumConnections request

*******************************************************************************/

public scope class GetNumConnectionsRequest : Protocol.GetNumConnections
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

        To be overriden by derivatives

        Returns:
            metadata that includes amount of established connections

    ***************************************************************************/

    override protected NumConnectionsData getConnectionsData ( )
    {
        NumConnectionsData data;
        data.address   = this.resources.node_info.node_item.Address;
        data.port      = this.resources.node_info.node_item.Port;
        data.num_conns = this.resources.node_info.num_open_connections;
        return data;
    }
}
