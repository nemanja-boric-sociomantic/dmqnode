/*******************************************************************************

    GetChannelSize request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release
                    August 2011: Fiber-based version

    authors:        Gavin Norman

*******************************************************************************/

module queuenode.request.GetChannelSizeRequest;


/*******************************************************************************

    Imports

*******************************************************************************/

private import Protocol = dmqproto.node.request.GetChannelSize;

private import queuenode.request.model.IDmqRequestResources;


/*******************************************************************************

    GetChannelSize request

*******************************************************************************/

public scope class GetChannelSizeRequest : Protocol.GetChannelSize
{
    /***************************************************************************
    
        Shared resource acquirer

    ***************************************************************************/

    private IDmqRequestResources resources;

    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        IDmqRequestResources resources )
    {
        super(reader, writer, resources);
        this.resources = resources;
    }

    /***************************************************************************

        Gets the metadata for specified channel. Overriden in
        actual implementors of queuenode protocol.

        Params:
            channel_name = name of channel to be queried

    ***************************************************************************/

    override protected ChannelSizeData getChannelData ( char[] channel_name )
    {
        ChannelSizeData data;

        data.address = this.resources.node_info.node_item.Address;
        data.port = this.resources.node_info.node_item.Port;

        auto storage_channel =
            *this.resources.channel_buffer in this.resources.storage_channels;
        if ( storage_channel !is null )
        {
            data.records = storage_channel.num_records;
            data.bytes = storage_channel.num_bytes;
        }

        return data;
    }
}

