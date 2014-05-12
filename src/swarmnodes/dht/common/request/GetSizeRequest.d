/*******************************************************************************

    Get size request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module swarmnodes.dht.common.request.GetSizeRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.dht.common.request.model.IRequest;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Get size request

*******************************************************************************/

public scope class GetSizeRequest : IRequest
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
        super(DhtConst.Command.E.GetSize, reader, writer, resources);
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

        // TODO: is there a need to send the addr/port? surely the client knows this anyway?
        this.writer.writeArray(this.resources.node_info.node_item.Address);
        this.writer.write(this.resources.node_info.node_item.Port);

        ulong records, bytes;

        foreach ( DhtStorageEngine channel; this.resources.storage_channels )
        {
            auto channel_records = channel.num_records;
            auto channel_bytes = channel.num_bytes;

            records += channel_records;
            bytes += channel_bytes;
        }

        this.writer.write(records);
        this.writer.write(bytes);
    }
}

