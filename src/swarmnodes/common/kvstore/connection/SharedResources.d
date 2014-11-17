/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        06/09/2012: Initial release

    authors:        Gavin Norman

    Key/value shared resource manager. Handles acquiring / relinquishing of
    global resources by active request handlers.

*******************************************************************************/

module swarmnodes.common.kvstore.connection.SharedResources;



/*******************************************************************************

    Imports

    Imports which are required by the KVConnectionResources struct, below, are
    imported publicly, as they are also needed in
    swarmnodes.common.kvstore.request.model.IKVRequestResources (which imports this
    module). This is done to simplify the process of modifying the fields of
    KVConnectionResources --  forgetting to import something into both modules
    is a common source of very confusing compile errors.

*******************************************************************************/

private import swarm.core.common.connection.ISharedResources;

public import ocean.io.select.client.FiberSelectEvent;
public import ocean.io.select.client.FiberTimerEvent;

public import swarm.core.common.request.helper.LoopCeder;

public import swarmnodes.common.kvstore.storage.IStepIterator;

public import swarmnodes.common.kvstore.connection.DhtClient;

public import swarm.dht.common.RecordBatcher;
public import swarm.dht.common.NodeRecordBatcher : NodeRecordBatcherMap;

public import swarmnodes.common.kvstore.request.params.RedistributeNode;



/*******************************************************************************

    Struct whose fields define the set of shared resources which can be acquired
    by a request. Each request can acquire a single instance of each field.

*******************************************************************************/

public struct KVConnectionResources
{
    char[] channel_buffer;
    char[] key_buffer;
    char[] key2_buffer;
    char[] filter_buffer;
    char[] batch_buffer;
    char[] value_buffer;
    hash_t[] hash_buffer;
    FiberSelectEvent event;
    FiberTimerEvent timer;
    LoopCeder loop_ceder;
    IStepIterator iterator;
    RecordBatcher batcher;
    RecordBatch record_batch;
    NodeRecordBatcherMap node_record_batch;
    RedistributeNode[] redistribute_node_buffer;
    DhtClient dht_client;
}



/*******************************************************************************

    Mix in a class called SharedResources which contains a free list for each of
    the fields of KVConnectionResources. The free lists are used by
    individual requests to acquire and relinquish resources required for
    handling.

*******************************************************************************/

mixin SharedResources_T!(KVConnectionResources);

