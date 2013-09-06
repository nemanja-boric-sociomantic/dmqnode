/*******************************************************************************

    Abstract base class for dht node requests.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        December 2010: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.dht.request.model.IRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import Core = swarm.core.node.request.model.IRequest;

private import src.mod.dht.request.model.IDhtRequestResources;

private import swarm.dht.DhtConst;

private import src.mod.dht.model.IDhtNodeInfo;

private import src.mod.dht.storage.model.DhtStorageChannels;
private import src.mod.dht.storage.model.DhtStorageEngine;

debug private import ocean.util.log.Trace;



public abstract scope class IRequest : Core.IRequest
{
    /***************************************************************************

        Aliases for the convenience of sub-classes, avoiding public imports.

    ***************************************************************************/

    public alias .DhtStorageChannels DhtStorageChannels;

    public alias .DhtStorageEngine DhtStorageEngine;

    public alias .IDhtNodeInfo IDhtNodeInfo;

    public alias .DhtConst DhtConst;

    public alias .IDhtRequestResources IDhtRequestResources;


    /***************************************************************************

        Code of command. Used to check for storage engine support.

    ***************************************************************************/

    private const DhtConst.Command.E cmd;


    /***************************************************************************

        Shared resources which might be required by the request.

    ***************************************************************************/

    protected const IDhtRequestResources resources;


    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( DhtConst.Command.E cmd, FiberSelectReader reader,
        FiberSelectWriter writer, IDhtRequestResources resources )
    {
        super(reader, writer);

        this.cmd = cmd;
        this.resources = resources;
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

