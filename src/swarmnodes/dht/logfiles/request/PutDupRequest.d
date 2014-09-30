/*******************************************************************************

    PutDup request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        August 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module swarmnodes.dht.logfiles.request.PutDupRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.dht.common.request.model.IPutSingleRequest;



/*******************************************************************************

    PutDup request

*******************************************************************************/

public scope class PutDupRequest : IPutSingleRequest
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
        super(DhtConst.Command.E.PutDup, reader, writer, resources);
    }


    /***************************************************************************

        Performs the put request on the storage channel request.

        Params:
            channel = channel to put to
            key = key to put
            value = value to put

    ***************************************************************************/

    protected void performRequest ( DhtStorageEngine channel, char[] key,
        char[] value )
    {
        channel.putDup(key, value);
    }
}

