/*******************************************************************************

    Abstract base class for queue node requests.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module swarmnodes.mod.queue.request.model.IRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import Core = swarm.core.node.request.model.IRequest;

private import swarmnodes.mod.queue.request.model.IQueueRequestResources;

private import swarm.queue.QueueConst;

private import swarmnodes.mod.queue.storage.model.QueueStorageChannels;

private import swarmnodes.mod.queue.model.IQueueNodeInfo;



/*******************************************************************************

    Queue node IRequest class

*******************************************************************************/

public abstract scope class IRequest : Core.IRequest
{
    /***************************************************************************

        Aliases for the convenience of sub-classes, avoiding public imports.

    ***************************************************************************/

    public alias .QueueStorageChannels QueueStorageChannels;

    public alias .IQueueNodeInfo IQueueNodeInfo;

    public alias .QueueConst QueueConst;

    public alias .IQueueRequestResources IQueueRequestResources;


    /***************************************************************************

        Shared resources which might be required by the request.

    ***************************************************************************/

    protected const IQueueRequestResources resources;


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
}

