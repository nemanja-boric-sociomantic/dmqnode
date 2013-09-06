/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        06/09/2012: Initial release

    authors:        Gavin Norman

    Queue shared resource manager. Handles acquiring / relinquishing of global
    resources by active request handlers.

*******************************************************************************/

module src.mod.queue.connection.SharedResources;



/*******************************************************************************

    Imports

    Imports which are required by the QueueConnectionResources struct, below,
    are imported publicly, as they are also needed in
    src.mod.dht.request.model.IQueueRequestResources (which imports this
    module). This is done to simplify the process of modifying the fields of
    QueueConnectionResources --  forgetting to import something into both
    modules is a common source of very confusing compile errors.

*******************************************************************************/

private import swarm.core.common.connection.ISharedResources;

public import swarm.core.common.request.helper.LoopCeder;

public import swarm.core.protocol.StringListReader;

public import ocean.io.select.event.FiberSelectEvent;



/*******************************************************************************

    Struct whose fields define the set of shared resources which can be acquired
    by a request. Each request can acquire a single instance of each field.

*******************************************************************************/

public struct QueueConnectionResources
{
    char[] channel_buffer;
    bool[] channel_flags_buffer;
    char[] value_buffer;
    FiberSelectEvent event;
    StringListReader string_list_reader;
    LoopCeder loop_ceder;
}



/*******************************************************************************

    Mix in a class called SharedResources which contains a free list for each of
    the fields of QueueConnectionResources. The free lists are used by
    individual requests to acquire and relinquish resources required for
    handling.

*******************************************************************************/

mixin SharedResources_T!(QueueConnectionResources);

