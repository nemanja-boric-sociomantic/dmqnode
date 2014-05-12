/*******************************************************************************

    Base class for a request over a specific channel.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module swarmnodes.dht.common.request.model.IChannelRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.dht.common.request.model.IRequest;

private import swarm.core.Const;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Channel request base class

*******************************************************************************/

public abstract scope class IChannelRequest : IRequest
{
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
        super.reader.readArray(*this.resources.channel_buffer);

        this.readRequestData_();
    }

    abstract protected void readRequestData_ ( );


    /***************************************************************************

        Performs this request. (Fiber method, after command validity has been
        confirmed.)

    ***************************************************************************/

    final protected void handle__ ( )
    {
        if ( validateChannelName(*this.resources.channel_buffer) )
        {
            this.handle___();
        }
        else
        {
            super.writer.write(DhtConst.Status.E.BadChannelName);
        }
    }


    /***************************************************************************

        Performs this request. (Fiber method, after command and channel validity
        have been confirmed.)

    ***************************************************************************/

    abstract protected void handle___ ( );
}

