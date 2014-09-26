/*******************************************************************************

    Abstract base class for queue node requests over a channel.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module swarmnodes.queue.request.model.IChannelRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.queue.request.model.IRequest;

private import swarm.core.Const;



/*******************************************************************************

    Queue node IChannelRequest class

*******************************************************************************/

public scope class IChannelRequest : IRequest
{
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
        this.reader.readArray(*this.resources.channel_buffer);

        this.readRequestData_();
    }

    abstract protected void readRequestData_ ( );


    /***************************************************************************

        Performs this request. (Fiber method, after command validity has been
        confirmed.)

    ***************************************************************************/

    final protected void handle_ ( )
    {
        if ( validateChannelName(*this.resources.channel_buffer) )
        {
            this.handle__();
        }
        else
        {
            super.writer.write(QueueConst.Status.E.BadChannelName);
        }
    }


    /***************************************************************************

        Performs this request. (Fiber method, after command and channel validity
        have been confirmed.)

    ***************************************************************************/

    abstract protected void handle__ ( );
}

