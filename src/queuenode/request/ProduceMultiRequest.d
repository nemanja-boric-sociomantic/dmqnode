/*******************************************************************************

    ProduceMulti request class.

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        30/08/2012: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module queuenode.request.ProduceMultiRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

private import Protocol = queueproto.node.request.ProduceMulti;

private import queuenode.request.model.IQueueRequestResources;

private import swarm.core.common.request.helper.LoopCeder;

/*******************************************************************************

    ProduceMulti request

*******************************************************************************/

public class ProduceMultiRequest : Protocol.ProduceMulti
{
    /***************************************************************************

        Shared resource acquirer

    ***************************************************************************/

    private IQueueRequestResources resources;
    
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
        super(reader, writer, resources);
        this.resources = resources;
    }

    /***************************************************************************

        Pushes a received record to one or more queues. To be overriden by
        an actual implementors of queuenode protocol.

        Params:
            channel_names = names of channels to push to
            value = record value to push

    ***************************************************************************/

    override protected void pushRecord ( char[][] channel_names, char[] value )
    {
        foreach ( channel; channel_names )
        {
            if ( this.resources.storage_channels.sizeLimitOk(
                    channel, value.length) )
            {
                auto storage_channel =
                    this.resources.storage_channels.getCreate(channel);
                if ( storage_channel !is null )
                {
                    storage_channel.push(value);
                }
            }
        }

        this.resources.loop_ceder.handleCeding();
    }
}
