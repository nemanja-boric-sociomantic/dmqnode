/*******************************************************************************

    Abstract base class for key/value node requests.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        December 2010: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module queuenode.common.kvstore.request.model.IRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import Core = swarm.core.node.request.model.IRequest;

private import queuenode.common.kvstore.request.model.IKVRequestResources;

private import swarm.dht.DhtConst;

private import queuenode.common.kvstore.node.IKVNodeInfo;

private import queuenode.common.kvstore.storage.KVStorageChannels;
private import queuenode.common.kvstore.storage.KVStorageEngine;

private import ocean.text.convert.Layout;



public abstract scope class IRequest : Core.IRequest
{
    /***************************************************************************

        Aliases for the convenience of sub-classes, avoiding public imports.

    ***************************************************************************/

    public alias .KVStorageChannels KVStorageChannels;

    public alias .KVStorageEngine KVStorageEngine;

    public alias .IKVNodeInfo IKVNodeInfo;

    public alias .DhtConst DhtConst;

    public alias .IKVRequestResources IKVRequestResources;


    /***************************************************************************

        Code of command. Used to check for storage engine support.

    ***************************************************************************/

    private const DhtConst.Command.E cmd;


    /***************************************************************************

        Shared resources which might be required by the request.

    ***************************************************************************/

    protected const IKVRequestResources resources;


    /***************************************************************************

        Constructor

        Params:
            cmd = command code
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( DhtConst.Command.E cmd, FiberSelectReader reader,
        FiberSelectWriter writer, IKVRequestResources resources )
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
        auto cmd_str = this.cmd in DhtConst.Command();

        dst.length = 0;
        Layout!(char).print(dst, "{} request", cmd_str ? *cmd_str : "?");
        return dst;
    }


    /***************************************************************************

        Fiber method. Checks whether the command is supported by the storage
        channels, and either handles it or returns the 'command not supported'
        status code to the client.

    ***************************************************************************/

    final protected void handle_ ( )
    {
        if ( this.resources.storage_channels.commandSupported(this.cmd) )
        {
            this.handle__();
        }
        else
        {
            this.writer.write(DhtConst.Status.E.NotSupported);
        }
    }


    /***************************************************************************

        Performs this request. (Fiber method, after command validity has been
        confirmed.)

    ***************************************************************************/

    abstract protected void handle__ ( );
}

