/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        13/09/2012: Initial release

    authors:        Gavin Norman

    Interface and base scope class containing getter methods to acquire
    resources needed by a key/value node request. Multiple calls to the same
    getter only result in the acquiring of a single resource of that type, so
    that the same resource is used over the life time of a request. When a
    request resource instance goes out of scope all required resources are
    automatically relinquished.

*******************************************************************************/

module queuenode.common.kvstore.request.model.IKVRequestResources;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.common.request.model.IRequestResources;

private import queuenode.common.kvstore.connection.SharedResources;

private import queuenode.common.kvstore.storage.KVStorageChannels;
private import queuenode.common.kvstore.storage.IStepIterator;

private import queuenode.common.kvstore.node.IKVNodeInfo;



/*******************************************************************************

    Mix in an interface called IRequestResources which contains a getter method
    for each type of acquirable resource, as defined by the SharedResources
    class (queuenode.common.kvstore.connection.SharedResources).

*******************************************************************************/

mixin IRequestResources_T!(SharedResources);



/*******************************************************************************

    Interface which extends the base IRequestResources, adding a couple of
    key/value-specific getters.

*******************************************************************************/

public interface IKVRequestResources : IRequestResources
{
    /***************************************************************************

        Local type re-definitions.

    ***************************************************************************/

    alias .FiberSelectEvent FiberSelectEvent;
    alias .LoopCeder LoopCeder;
    alias .KVStorageChannels KVStorageChannels;
    alias .IKVNodeInfo IKVNodeInfo;
    alias .IStepIterator IStepIterator;


    /***************************************************************************

        Storage channels getter.

    ***************************************************************************/

    KVStorageChannels storage_channels ( );


    /***************************************************************************

        Node info getter.

    ***************************************************************************/

    IKVNodeInfo node_info ( );
}



/*******************************************************************************

    Mix in a scope class called RequestResources which implements
    IRequestResources. Note that this class does not implement the additional
    methods required by IKVRequestResources -- this is done by the template in
    queuenode.common.kvstore.connection.KVConnectionHandler, which must be
    mixed into the concrete connection handler class.

*******************************************************************************/

mixin RequestResources_T!(SharedResources);

