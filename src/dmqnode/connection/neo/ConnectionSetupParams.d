/*******************************************************************************

    Parameters passed to each DMQ neo connection handler.

    Copyright (c) 2016 sociomantic labs. All rights reserved

*******************************************************************************/

module dmqnode.connection.neo.ConnectionSetupParams;

/*******************************************************************************

    Imports

*******************************************************************************/

import dmqnode.connection.neo.SharedResources;

import ocean.transition;

import swarm.core.neo.node.ConnectionHandler;

/*******************************************************************************

    Parameters passed to each DMQ neo connection handler. Extends the core neo
    connection handler setup params:
        1. Stores a reference to the storage channels.
        2. Constructs a shared resources instance and passes it to the super
           class.

*******************************************************************************/

public final class ConnectionSetupParams : ConnectionHandler.SharedParams
{
    import dmqnode.storage.model.StorageChannels;

    import ocean.io.select.EpollSelectDispatcher;

    /***************************************************************************

        Reference to the storage channels which the requests are operating on.

    ***************************************************************************/

    public StorageChannels storage_channels;

    /***************************************************************************

        Constructor.

        Params:
            epoll = epoll dispatcher used by the node
            storage_channels = DMQ node's storage channels
            cmd_handlers = table of handler functions by command
            credentials  = authentication keys by client name
            TODO

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, StorageChannels storage_channels,
        ConnectionHandler.CmdHandlers cmd_handlers, Key[istring] credentials,
        bool no_delay )
    {
        super(epoll, new SharedResources(storage_channels), cmd_handlers,
            credentials, no_delay);

        this.storage_channels = storage_channels;
    }
}
