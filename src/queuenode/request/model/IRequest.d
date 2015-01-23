/*******************************************************************************

    Abstract base class for queue node requests.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module queuenode.request.model.IRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import Core = swarm.core.node.request.model.IRequest;

private import queuenode.request.model.IQueueRequestResources;

private import swarm.queue.QueueConst;

private import queuenode.storage.model.QueueStorageChannels;

private import queuenode.node.IQueueNodeInfo;

private import tango.text.convert.Format;



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

        Code of command.

    ***************************************************************************/

    private const QueueConst.Command.E cmd;


    /***************************************************************************

        Shared resources which might be required by the request.

    ***************************************************************************/

    protected const IQueueRequestResources resources;


    /***************************************************************************

        Constructor

        Params:
            cmd = command code
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( QueueConst.Command.E cmd, FiberSelectReader reader,
        FiberSelectWriter writer, IQueueRequestResources resources )
    {
        super(reader, writer);

        this.cmd = cmd;
        this.resources = resources;
    }


    /***************************************************************************

        Formats a description of this command into the provided buffer. The
        default implementation simply formats the name of the command. Derived
        request classes may override and add more detailed information.

        Params:
            dst = buffer to format description into

        Returns:
            description of command (slice of dst)

    ***************************************************************************/

    override public char[] description ( ref char[] dst )
    {
        auto cmd_str = this.cmd in QueueConst.Command();

        dst.length = 0;
        Format.format(dst, "{} request", cmd_str ? *cmd_str : "?");
        return dst;
    }
}

