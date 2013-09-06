/*******************************************************************************

    Get responsible range request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.dht.request.GetResponsibleRangeRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.dht.request.model.IRequest;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Get responsible range request

*******************************************************************************/

public scope class GetResponsibleRangeRequest : IRequest
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
        super(DhtConst.Command.E.GetResponsibleRange, reader, writer, resources);
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

        Performs this request. (Fiber method, after command validity has been
        confirmed.)

    ***************************************************************************/

    protected void handle__ ( )
    {
        this.writer.write(DhtConst.Status.E.Ok);
        this.writer.write(this.resources.node_info.min_hash);
        this.writer.write(this.resources.node_info.max_hash);
    }
}

