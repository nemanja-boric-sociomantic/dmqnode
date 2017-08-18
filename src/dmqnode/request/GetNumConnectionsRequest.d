/*******************************************************************************

    GetNumConnections request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

*******************************************************************************/

module dmqnode.request.GetNumConnectionsRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

import dmqnode.request.model.IDmqRequestResources;

import Protocol = dmqproto.node.request.GetNumConnections;

/*******************************************************************************

    GetNumConnections request

*******************************************************************************/

public scope class GetNumConnectionsRequest : Protocol.GetNumConnections
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
