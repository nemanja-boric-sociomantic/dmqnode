/*******************************************************************************

    Get supported commands request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.dht.request.GetSupportedCommandsRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.dht.request.model.IRequest;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Get supported commands request

*******************************************************************************/

public scope class GetSupportedCommandsRequest : IRequest
{
    /***************************************************************************

        Constructor
    
        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        IDhtRequestResources resources )
    {
        super(DhtConst.Command.E.GetSupportedCommands, reader, writer, resources);
    }


    /***************************************************************************
    
        Reads any data from the client which is required for the request. If the
        request is invalid in some way (the channel name is invalid, or the 
        command is not supported) then the command can be simply not executed,
        and all client data has been read, leaving the read buffer in a clean
        state ready for the next request.
    
    ***************************************************************************/
    
    protected void readRequestData ( )
    {
    }


    /***************************************************************************

        Performs this request. (Fiber method.)

    ***************************************************************************/

    protected void handle__ ( )
    {
        this.writer.write(DhtConst.Status.E.Ok);

        DhtConst.Command.E[DhtConst.Command.names.length] supported_commands;
        size_t len;

        foreach ( descr, cmd; DhtConst.Command() )
        {
            auto dht_cmd = cast(DhtConst.Command.E)cmd;
            if ( this.resources.storage_channels.commandSupported(dht_cmd) )
            {
                supported_commands[len++] = dht_cmd;
            }
        }

        this.writer.writeArray(supported_commands[0 .. len]);
    }
}

