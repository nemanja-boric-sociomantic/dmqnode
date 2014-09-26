/*******************************************************************************

    ProduceMulti request class.

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        30/08/2012: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module swarmnodes.queue.request.ProduceMultiRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.queue.request.model.IMultiChannelRequest;

private import swarm.core.common.request.helper.LoopCeder;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    ProduceMulti request

*******************************************************************************/

public class ProduceMultiRequest : IMultiChannelRequest
{
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
        super(QueueConst.Command.E.ProduceMulti, reader, writer, resources);
    }


    /***************************************************************************

        Reads any data from the client which is required for the request. If the
        request is invalid in some way (the channel name is invalid, or the
        command is not supported) then the command can be simply not executed,
        and all client data has been read, leaving the read buffer in a clean
        state ready for the next request.

    ***************************************************************************/

    protected void readRequestData_ ( )
    {
    }


    /***************************************************************************

        Performs this request. (Fiber method.)

    ***************************************************************************/

    protected void handle_ ( )
    {
        this.writer.write(QueueConst.Status.E.Ok);
        this.writer.flush; // flush write buffer, so client can start sending

        do
        {
            this.reader.readArray(*this.resources.value_buffer);

            this.resources.node_info.handledRecord();

            if ( (*this.resources.value_buffer).length )
            {
                foreach ( channel; this.channels )
                {
                    if ( this.resources.storage_channels.sizeLimitOk(
                            channel, (*this.resources.value_buffer).length) )
                    {
                        auto storage_channel =
                            this.resources.storage_channels.getCreate(channel);
                        if ( storage_channel !is null )
                        {
                            storage_channel.push(*this.resources.value_buffer);
                        }
                    }
                }

                this.resources.loop_ceder.handleCeding();
            }
        }
        while ( (*this.resources.value_buffer).length );
    }
}

