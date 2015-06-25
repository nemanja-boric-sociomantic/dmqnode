/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        13/09/2012: Initial release

    authors:        Gavin Norman

    Interface and base scope class containing getter methods to acquire
    resources needed by a DMQ node request. Multiple calls to the same getter
    only result in the acquiring of a single resource of that type, so that the
    same resource is used over the life time of a request. When a request
    resource instance goes out of scope all required resources are automatically
    relinquished.

*******************************************************************************/

module queuenode.request.model.IDmqRequestResources;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.common.request.model.IRequestResources;

private import queuenode.connection.SharedResources;
private import queuenode.storage.model.StorageChannels;
private import queuenode.node.IDmqNodeInfo;

private import dmqproto.node.request.model.DmqCommand;

/*******************************************************************************

    Mix in an interface called IRequestResources which contains a getter method
    for each type of acquirable resource, as defined by the SharedResources
    class (queuenode.connection.SharedResources).

*******************************************************************************/

mixin IRequestResources_T!(SharedResources);



/*******************************************************************************

    Interface which extends the base IRequestResources, adding a couple of
    DMQ-specific getters.

*******************************************************************************/

public interface IDmqRequestResources : IRequestResources, DmqCommand.Resources
{
    /***************************************************************************

        Local type re-definitions.

    ***************************************************************************/

    alias .FiberSelectEvent FiberSelectEvent;
    alias .StringListReader StringListReader;
    alias .LoopCeder LoopCeder;
    alias .StorageChannels StorageChannels;
    alias .IDmqNodeInfo IDmqNodeInfo;


    /***************************************************************************

        Storage channels getter.

    ***************************************************************************/

    StorageChannels storage_channels ( );


    /***************************************************************************

        Node info getter.

    ***************************************************************************/

    IDmqNodeInfo node_info ( );
}



/*******************************************************************************

    Mix in a scope class called RequestResources which implements
    IRequestResources. Note that this class does not implement the additional
    methods required by IDmqRequestResources -- this is done in
     dmqnode.connection.ConnectionHandler.

*******************************************************************************/

mixin RequestResources_T!(SharedResources);
