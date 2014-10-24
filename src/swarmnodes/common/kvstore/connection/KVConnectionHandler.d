/*******************************************************************************

    Copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

    Connection handler for key-value nodes.

    Currently only the connection setup params class is shared.

*******************************************************************************/

module swarmnodes.common.kvstore.connection.KVConnectionHandler;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.connection.ConnectionHandler : ConnectionSetupParams;

private import swarmnodes.common.kvstore.connection.SharedResources;

private import swarmnodes.common.kvstore.storage.KVStorageChannels;



/*******************************************************************************

    Key/value node connection handler setup class. Passed to the key/value
    connection handler constructor.

    TODO: enable HMAC authentication by deriving from HmacAuthConnectionSetupParams

*******************************************************************************/

public class KVConnectionSetupParams : ConnectionSetupParams
{
    /***************************************************************************

        Reference to the storage channels which the requests are operating on.

    ***************************************************************************/

    public KVStorageChannels storage_channels;


    /***************************************************************************

        Reference to the request resources pool shared between all connection
        handlers.

    ***************************************************************************/

    public SharedResources shared_resources;
}

