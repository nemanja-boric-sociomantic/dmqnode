/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        06/09/2012: Initial release

    authors:        Gavin Norman

    Dht shared resource manager. Handles acquiring / relinquishing of global
    resources by active request handlers.

*******************************************************************************/

module swarmnodes.dht.connection.SharedResources;



/*******************************************************************************

    Imports

    Imports which are required by the DhtConnectionResources struct, below, are
    imported publicly, as they are also needed in
    swarmnodes.dht.request.model.IDhtRequestResources (which imports this
    module). This is done to simplify the process of modifying the fields of
    DhtConnectionResources --  forgetting to import something into both modules
    is a common source of very confusing compile errors.

*******************************************************************************/

private import swarm.core.common.connection.ISharedResources;

public import ocean.io.select.client.FiberSelectEvent;

public import swarm.core.common.request.helper.LoopCeder;

public import swarmnodes.dht.storage.model.IStepIterator;

public import swarm.dht.common.RecordBatcher2;



/*******************************************************************************

    Struct whose fields define the set of shared resources which can be acquired
    by a request. Each request can acquire a single instance of each field.

*******************************************************************************/

public struct DhtConnectionResources
{
    char[] channel_buffer;
    char[] key_buffer;
    char[] key2_buffer;
    char[] filter_buffer;
    char[] batch_buffer;
    char[] value_buffer;
    hash_t[] hash_buffer;
    FiberSelectEvent event;
    LoopCeder loop_ceder;
    IStepIterator iterator;
    RecordBatcher batcher;
}



/*******************************************************************************

    Mix in a class called SharedResources which contains a free list for each of
    the fields of DhtConnectionResources. The free lists are used by
    individual requests to acquire and relinquish resources required for
    handling.

*******************************************************************************/

mixin SharedResources_T!(DhtConnectionResources);

