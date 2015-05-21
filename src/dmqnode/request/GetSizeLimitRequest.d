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

    With the queue node disk overflow there is no size
    limit any more so the GetSizeLimit command is obsolete, and
    GetSizeLimitRequest is a no-op request class that will be deleted when the
    GetSizeLimit command is removed from the queue protocol.

*******************************************************************************/

public scope class GetSizeLimitRequest : Protocol.GetSizeLimit
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

