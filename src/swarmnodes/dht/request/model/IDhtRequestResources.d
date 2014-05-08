/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        13/09/2012: Initial release

    authors:        Gavin Norman

    Interface and base scope class containing getter methods to acquire
    resources needed by a dht node request. Multiple calls to the same getter
    only result in the acquiring of a single resource of that type, so that the
    same resource is used over the life time of a request. When a request
    resource instance goes out of scope all required resources are automatically
    relinquished.

*******************************************************************************/

module swarmnodes.dht.request.model.IDhtRequestResources;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.common.request.model.IRequestResources;

private import swarmnodes.dht.connection.SharedResources;

private import swarmnodes.dht.storage.model.DhtStorageChannels;
private import swarmnodes.dht.storage.model.IStepIterator;

private import swarmnodes.dht.node.IDhtNodeInfo;



/*******************************************************************************

    Mix in an interface called IRequestResources which contains a getter method
    for each type of acquirable resource, as defined by the SharedResources
    class (swarmnodes.dht.connection.SharedResources).

*******************************************************************************/

mixin IRequestResources_T!(SharedResources);



/*******************************************************************************

    Interface which extends the base IRequestResources, adding a couple of
    dht-specific getters.

*******************************************************************************/

public interface IDhtRequestResources : IRequestResources
{
    /***************************************************************************

        Local type re-definitions.

    ***************************************************************************/

    alias .FiberSelectEvent FiberSelectEvent;
    alias .LoopCeder LoopCeder;
    alias .DhtStorageChannels DhtStorageChannels;
    alias .IDhtNodeInfo IDhtNodeInfo;
    alias .IStepIterator IStepIterator;


    /***************************************************************************

        Storage channels getter.

    ***************************************************************************/

    DhtStorageChannels storage_channels ( );


    /***************************************************************************

        Node info getter.

    ***************************************************************************/

    IDhtNodeInfo node_info ( );
}



/*******************************************************************************

    Mix in a scope class called RequestResources which implements
    IRequestResources. Note that this class does not implement the additional
    methods required by IDhtRequestResources -- this is done in
     swarmnodes.dht.connection.DhtConnectionHandler.

*******************************************************************************/

mixin RequestResources_T!(SharedResources);

