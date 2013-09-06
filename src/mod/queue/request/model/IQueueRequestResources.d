/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        13/09/2012: Initial release

    authors:        Gavin Norman

    Interface and base scope class containing getter methods to acquire
    resources needed by a queue node request. Multiple calls to the same getter
    only result in the acquiring of a single resource of that type, so that the
    same resource is used over the life time of a request. When a request
    resource instance goes out of scope all required resources are automatically
    relinquished.

*******************************************************************************/

module src.mod.queue.request.model.IQueueRequestResources;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.common.request.model.IRequestResources;

private import src.mod.queue.connection.SharedResources;

private import src.mod.queue.storage.model.QueueStorageChannels;

private import src.mod.queue.model.IQueueNodeInfo;



/*******************************************************************************

    Mix in an interface called IRequestResources which contains a getter method
    for each type of acquirable resource, as defined by the SharedResources
    class (src.mod.queue.connection.SharedResources).

*******************************************************************************/

mixin IRequestResources_T!(SharedResources);



/*******************************************************************************

    Interface which extends the base IRequestResources, adding a couple of
    queue-specific getters.

*******************************************************************************/

public interface IQueueRequestResources : IRequestResources
{
    /***************************************************************************

        Local type re-definitions.

    ***************************************************************************/

    alias .FiberSelectEvent FiberSelectEvent;
    alias .StringListReader StringListReader;
    alias .LoopCeder LoopCeder;
    alias .QueueStorageChannels QueueStorageChannels;
    alias .IQueueNodeInfo IQueueNodeInfo;


    /***************************************************************************

        Storage channels getter.

    ***************************************************************************/

    QueueStorageChannels storage_channels ( );


    /***************************************************************************

        Node info getter.

    ***************************************************************************/

    IQueueNodeInfo node_info ( );
}



/*******************************************************************************

    Mix in a scope class called RequestResources which implements
    IRequestResources. Note that this class does not implement the additional
    methods required by IQueueRequestResources -- this is done in
     src.mod.queue.QueueConnectionHandler.

*******************************************************************************/

mixin RequestResources_T!(SharedResources);
