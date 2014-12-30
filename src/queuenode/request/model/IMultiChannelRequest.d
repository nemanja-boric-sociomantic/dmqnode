/*******************************************************************************

    Abstract base class for queue node requests over multiple channels.

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        30/08/2012: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module queuenode.request.model.IMultiChannelRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import queuenode.request.model.IRequest;

private import swarm.core.protocol.StringListReader;



/*******************************************************************************

    Base class for requests that operate over multiple channels.

*******************************************************************************/

public scope class IMultiChannelRequest : IRequest
{
    /***************************************************************************

        Channels being pushed to (slice into the buffer in
        resources.string_list_reader).

    ***************************************************************************/

    protected char[][] channels;


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
        super(cmd, reader, writer, resources);
    }


    /***************************************************************************

        Reads any data from the client which is required for the request. If the
        request is invalid in some way (the channel name is invalid, or the
        command is not supported) then the command can be simply not executed,
        and all client data has been read, leaving the read buffer in a clean
        state ready for the next request.

    ***************************************************************************/

    final protected void readRequestData ( )
    {
        auto read_channels = this.resources.string_list_reader;
        this.channels = read_channels();
        this.readRequestData_();
    }

    abstract protected void readRequestData_ ( );
}

