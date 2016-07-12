/*******************************************************************************

    Copyright (c) 2016 sociomantic labs. All rights reserved

    DMQ shared resource manager. Handles acquiring / relinquishing of global
    resources by active request handlers.

    The structure of this module is currently based on the old
    ConnectionHandler/SetupParams using a SetupParams class hierarchy. With the
    Neo structure the request shared resources can be separated from the
    connection setup parameters, see
    https://github.com/sociomantic/swarm/issues/605

*******************************************************************************/

module dmqnode.connection.neo.SharedResources;

/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.neo.node.ConnectionHandler;

/*******************************************************************************

    DMQ node neo connection handler setup class. Passed to the DMQ neo
    connection handler constructor and to each request handler function.

*******************************************************************************/

public class NeoConnectionSetupParams : ConnectionHandler.SetupParams
{
    private import dmqnode.storage.model.StorageChannels;
    private import ocean.io.select.EpollSelectDispatcher;

    private import swarm.core.common.request.model.IRequestResources;
    private import swarm.core.common.connection.ISharedResources;

    /***************************************************************************

        Reference to the storage channels which the requests are operating on.

    ***************************************************************************/

    public StorageChannels storage_channels;

    /***************************************************************************

        Struct whose fields define the set of shared resources which can be
        acquired by a request. Each request can acquire a single instance of
        each field.

    ***************************************************************************/

    private static struct ConnectionResources
    {
        char[] value_buffer;
    }

    /***************************************************************************

        Mix in a class called SharedResources which contains a free list for
        each of the fields of DmqConnectionResources. The free lists are used by
        individual requests to acquire and relinquish resources required for
        handling.

    ***************************************************************************/

    static mixin SharedResources_T!(ConnectionResources);

    /***************************************************************************

        Mix in an interface called IRequestResources which contains a getter
        method for each type of acquirable resource, as defined by the
        SharedResources class (dmqnode.connection.SharedResources).

    ***************************************************************************/

    static mixin IRequestResources_T!(SharedResources);

    /***************************************************************************

        Mix in a scope class called RequestResources which implements
        IRequestResources. Note that this class does not implement the
        additional methods required by IDmqRequestResources -- this is done in
        dmqnode.connection.ConnectionHandler.

    ***************************************************************************/

    static mixin RequestResources_T!(SharedResources);

    /***************************************************************************

        Reference to the request resources pool shared between all connection
        handlers.

    ***************************************************************************/

    private SharedResources shared_resources;

    /***************************************************************************

        Constructor.

        Params:
            epoll = epoll instance (currently unused)
            storage_channels = storage channels (currently unused)
            cmd_handlers = table of handler functions by request codes
            credentials  = authentication keys by client name

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, StorageChannels storage_channels,
                  CmdHandlers cmd_handlers, Key[char[]] credentials )
    {
        super(cmd_handlers, credentials);
        this.epoll = epoll;
        this.storage_channels = storage_channels;
        this.shared_resources = new SharedResources;
    }

    /***************************************************************************

        Helper class adding a couple of DMQ-specific getters as well as the
        resource acquiring getters required by the DmqCommand protocol base
        class. The resources are acquired from the shared
        resources instance which is passed to ConnectionHandler's
        constructor (in the ConnectionSetupParams instance). Acquired
        resources are automatically relinquished in the destructor.

        Note that it is assumed that each request will own at most one of each
        resource type (it is not possible, for example, to acquire two value
        buffers).

    ***************************************************************************/

    public /*scope*/ class DmqRequestResources: RequestResources
    {
        /***********************************************************************

            Constructor.

        ***********************************************************************/

        public this ( )
        {
            super(this.outer.shared_resources);
        }

        /***********************************************************************

            Value buffer newer.

        ***********************************************************************/

        override protected char[] new_value_buffer ( )
        {
            return new char[50];
        }
    }
}
