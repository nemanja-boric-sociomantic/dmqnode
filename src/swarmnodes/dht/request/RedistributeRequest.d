/*******************************************************************************

    Redistribute request class.

    copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

    A Redistribute request instructs the node to change its hash responsibility
    range and to forward any records for which it is no longer responsible to
    another node. The client sending this request is required to include a list
    of replacement nodes, along with their hash responsibility ranges.

*******************************************************************************/

module swarmnodes.dht.request.RedistributeRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.common.kvstore.request.model.IRequest;

private import swarmnodes.common.kvstore.request.params.RedistributeNode;

private import swarmnodes.common.kvstore.connection.DhtClient;

private import swarmnodes.common.kvstore.storage.IStepIterator;

private import Hash = swarm.core.Hash;

private import swarm.dht.DhtConst : HashRange;

private import swarm.dht.common.NodeRecordBatcher;

private import swarm.dht.client.registry.model.IDhtNodeRegistryInfo;

private import swarm.dht.client.connection.model.IDhtNodeConnectionPoolInfo;

private import ocean.core.Array : copy;

private import tango.util.log.Log;

private import tango.time.StopWatch;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("swarmnodes.dht.request.RedistributeRequest");
}



/*******************************************************************************

    Redistribute request

*******************************************************************************/

public scope class RedistributeRequest : IRequest
{
    /***************************************************************************

        Only a single Redistribute request may be handled at a time. This global
        counter is incremented in the ctor and decremented in the dtor. The
        handle__() method checks that is it == 1, and returns an error code to
        the client otherwise.

    ***************************************************************************/

    private static uint instance_count;


    /***************************************************************************

        Code indicating the result of forwarding a record. See forwardRecord().

    ***************************************************************************/

    private enum ForwardResult
    {
        None,
        Batched,
        SentBatch,
        SentSingle,
        SendError
    }


    /***************************************************************************

        New minimum and maximum hash range for this node. Received from the
        client which sent the request.

    ***************************************************************************/

    private hash_t min, max;


    /***************************************************************************

        Flag indicating that one of the hash ranges received from the client is
        invalid. The request will not be handled, in this case.

    ***************************************************************************/

    private bool bad_range;


    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        IKVRequestResources resources )
    {
        this.instance_count++;

        super(DhtConst.Command.E.Redistribute, reader, writer, resources);
    }


    /***************************************************************************

        Destructor. Decrements the global 'instance_count', potentially allowing
        another Redistribute request to start.

    ***************************************************************************/

    ~this ( )
    {
        this.instance_count--;
    }


    /***************************************************************************

        Reads any data from the client which is required for the request. If the
        request is invalid in some way (the channel name is invalid, or the
        command is not supported) then the command can be simply not executed,
        and all client data has been read, leaving the read buffer in a clean
        state ready for the next request.

    ***************************************************************************/

    protected void readRequestData ( )
    {
        this.reader.read(this.min);
        this.reader.read(this.max);
        log.trace("New hash range: 0x{:x16}..0x{:x16}", this.min, this.max);
        if ( !HashRange.isValid(this.min, this.max) )
        {
            this.bad_range = true;
        }

        (*this.resources.redistribute_node_buffer).length = 0;

        while ( true )
        {
            (*this.resources.redistribute_node_buffer).length =
                (*this.resources.redistribute_node_buffer).length + 1;
            RedistributeNode* next =
                &(*this.resources.redistribute_node_buffer)[$-1];

            this.reader.readArray(next.node.Address);
            if ( next.node.Address.length == 0 ) break;
            this.reader.read(next.node.Port);

            hash_t min, max;
            this.reader.read(min);
            this.reader.read(max);
            log.trace("Forward to node {}:{} 0x{:x16}..0x{:x16}",
                next.node.Address, next.node.Port, min, max);
            if ( HashRange.isValid(min, max) )
            {
                next.range = HashRange(min, max);
            }
            else
            {
                this.bad_range = true;
            }
        }

        // Cut off final "end of flow" marker
        (*this.resources.redistribute_node_buffer).length =
            (*this.resources.redistribute_node_buffer).length - 1;
    }


    /***************************************************************************

        Performs this request. (Fiber method.)

    ***************************************************************************/

    protected void handle__ ( )
    {
        if ( this.instance_count > 1 )
        {
            log.error("Attempt to start multiple simultaneous Redistribute requests");
            this.writer.write(DhtConst.Status.E.Error);
            return;
        }

        if ( this.bad_range )
        {
            log.error("Received invalid hash range from client");
            this.writer.write(DhtConst.Status.E.Error);
            return;
        }

        // TODO: check that the new range of this node plus the ranges of the
        // other nodes completely cover (are a superset of) the old range of
        // this node. Return an error code otherwise.
        // This will require the list of new nodes to be sorted by hash range.

        this.writer.write(DhtConst.Status.E.Ok);

        log.info("Setting hash range: 0x{:X16}..0x{:X16}", this.min, this.max);
        this.resources.storage_channels.setHashRange(this.min, this.max);

        // set up dht client
        auto client = this.resources.dht_client;
        foreach ( node; *this.resources.redistribute_node_buffer )
        {
            client.addNode(node.node, node.range);
        }

        this.resources.node_record_batch.reset(
            cast(IDhtNodeRegistryInfo)client.nodes);

        // iterate over channels, redistributing data
        foreach ( channel; this.resources.storage_channels )
        {
            this.handleChannel(client, channel);
        }
    }


    /***************************************************************************

        Iterates over the given storage engine, forwarding those records for
        which this node is no longer responsible to the appropriate node. If an
        error occurs while forwarding one or more records, those records are
        kept in the storage engine and the complete iteration is retried.

        Params:
            client = dht client instance to send data to other nodes
            channel = storage channel to process

    ***************************************************************************/

    private void handleChannel ( DhtClient client, KVStorageEngine channel )
    {
        log.info("Redistributing channel '{}'", channel.id);

        this.resources.iterator.setStorage(channel);

        const ulong log_interval = 5;
        ulong next_log_time;
        StopWatch time;
        if ( log.enabled(log.Trace) )
        {
            time.start();
        }

        bool error_during_iteration;
        do
        {
            error_during_iteration = false;
            ulong num_records_before = channel.num_records;
            ulong num_records_iterated;

            channel.getAll(this.resources.iterator);

            while ( !this.resources.iterator.lastKey )
            {
                bool remove_record;
                DhtConst.NodeItem node;

                if ( this.recordShouldBeForwarded(this.resources.iterator.key,
                    client, node) )
                {
                    auto result = this.forwardRecord(client, channel,
                        this.resources.iterator.key,
                        this.resources.iterator.value, node);
                    with ( ForwardResult ) switch ( result )
                    {
                        case SentSingle:
                            remove_record = true;
                            break;
                        case Batched:
                        case SentBatch:
                            break;
                        case SendError:
                            error_during_iteration = true;
                            break;
                        default:
                            assert(false);
                    }
                }

                this.advanceIteration(this.resources.iterator, remove_record,
                    channel);
                num_records_iterated++;

                if ( log.enabled(log.Trace) )
                {
                    auto sec = time.microsec / 1_000_000;

                    if ( sec >= next_log_time )
                    {
                        log.trace("Progress redistributing channel '{}': {}/{} "
                            "records iterated, channel now contains {} records",
                            channel.id, num_records_iterated, num_records_before,
                            channel.num_records);
                        next_log_time = sec + log_interval;
                    }
                }
            }

            if ( !this.flushBatches(client, channel) )
            {
                error_during_iteration = true;
            }

            if ( error_during_iteration )
            {
                const uint retry_s = 2;

                log.error("Finished redistributing channel '{}': {}/{} records "
                    "iterated, channel now contains {} records, "
                    " (error occurred during iteration over channel, retrying in {}s)",
                    channel.id, num_records_iterated, num_records_before,
                    channel.num_records, retry_s);

                this.resources.timer.wait(retry_s);
            }
            else
            {
                log.info("Finished redistributing channel '{}': {}/{} records "
                    "iterated, channel now contains {} records",
                    channel.id, num_records_iterated, num_records_before,
                    channel.num_records);
            }
        }
        while ( error_during_iteration );
    }


    /***************************************************************************

        Determines whether the specified record should be forwarded to another
        node of whether this node is still responsible for it. If it should be
        forwarded, the output value node is set to the address/port of the node
        which should receive it.

        Params:
            key = record key
            client = dht client instance (for node registry)
            node = out value which receives the address/port of the node which
                is responsible for this record, if it should be forwarded

        Returns:
            true if this record should be forwarded (in which case the address
            and port of the node to which it should be sent are stored in the
            out value node), or false if it should be kept by this node

    ***************************************************************************/

    private bool recordShouldBeForwarded ( char[] key, DhtClient client,
        out DhtConst.NodeItem node )
    {
        auto hash = Hash.straightToHash(key);
        foreach ( n; client.nodes )
        {
            auto dht_node = cast(IDhtNodeConnectionPoolInfo)n;
            if ( Hash.isWithinNodeResponsibility(
                hash, dht_node.min_hash, dht_node.max_hash) )
            {
                node.Address = n.address;
                node.Port = n.port;
                return true;
            }
        }

        return false;
    }


    /***************************************************************************

        Relocates a record by adding it to a batch to be compressed and sent to
        the specified node. If the current batch of records to that node is
        full, then sendBatch() is called, sending the whole batch to the node.
        In the obscure case of a record which is too big to fit inside the batch
        buffer (even if empty), the individual record is sent uncompressed,
        using a standard Put request.

        Params:
            client = dht client instance to send data to other nodes
            channel = name of storage channel to which the record belongs
            key = record key
            value = record value
            node = address/port of node to which record should be forwarded

        Returns:
            enum value indicating whether the record was added to a batch, sent
            individually, sent as part of a batch, or sent and encountered an
            I/O error

    ***************************************************************************/

    private ForwardResult forwardRecord ( DhtClient client, KVStorageEngine channel,
        char[] key, char[] value, DhtConst.NodeItem node )
    {
        auto batch = this.resources.node_record_batch[node];

        bool fits, too_big;
        fits = batch.fits(key, value, too_big);

        if ( too_big )
        {
            log.warn("Forwarding large record {} ({} bytes) individually", key,
                value.length);
            return this.sendRecord(client, key, value, channel)
                ? ForwardResult.SentSingle : ForwardResult.SendError;
        }
        else
        {
            ForwardResult result = ForwardResult.Batched;

            if ( !fits )
            {
                result = this.sendBatch(client, batch, channel)
                    ? ForwardResult.SentBatch : ForwardResult.SendError;

                // The batch is always cleared. If an error occurred, we just
                // retry the whole iteration.
                batch.clear();

                assert(batch.fits(key, value));
            }

            auto add_result = batch.add(key, value);
            assert(add_result == add_result.Added);

            return result;
        }
    }


    /***************************************************************************

        Called at the end of an iteration over a channel. Flushes any partially
        built-up batches of records to the appropriate nodes.

        Params:
            client = dht client instance to send data to other nodes
            channel = storage channel to which the records belong

        Returns:
            true if flushing succeeded, false if an error occurred during the
            forwarding of a batch

    ***************************************************************************/

    private bool flushBatches ( DhtClient client, KVStorageEngine channel )
    {
        bool send_error;

        foreach ( node; client.nodes )
        {
            auto node_item = DhtConst.NodeItem(node.address, node.port);
            auto batch = this.resources.node_record_batch[node_item];

            if ( batch.length )
            {
                if ( !this.sendBatch(client, batch, channel) )
                {
                    send_error = true;
                }
                batch.clear();
            }
        }

        return !send_error;
    }


    /***************************************************************************

        Compresses and forwards the specified batch of records to the node which
        is now responsible for it.

        If the batch is sent successfully, the records it contained are removed
        from the storage engine. Upon error, do not attempt to retry sending the
        records immediately -- the return value indicates that the complete
        iteration over this channel's data should be repeated (see
        handleChannel()).

        Params:
            client = dht client instance to send data to other nodes
            batch = batch of records to compress and send
            channel = storage channel to which the records belong

        Returns:
            true if the batch was successfully forwarded or false if an error
            occurred

    ***************************************************************************/

    private bool sendBatch ( DhtClient client, NodeRecordBatcher batch,
        KVStorageEngine channel )
    {
        bool error;

        NodeRecordBatcher put_dg ( client.RequestContext )
        {
            return batch;
        }

        void notifier ( client.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                log.error("Error while sending batch of {} records to channel '{}': {}",
                    batch.length, channel.id, info.message(client.msg_buf));
                error = true;
            }
        }

        client.perform(this.reader.fiber, client.putBatch(batch.address,
            batch.port, channel.id, &put_dg, &notifier));

        // Remove successfully sent records from channel
        if ( !error )
        {
            foreach ( hash; batch.batched_hashes )
            {
                Hash.toString(hash, *this.resources.key_buffer);
                channel.remove(*this.resources.key_buffer);
            }
        }

        return !error;
    }


    /***************************************************************************

        Forwards the specified record to the node which is now responsible for
        it.

        If the record is sent successfully, it will be removed from the storage
        engine after advancing the iterator (see advanceIteration()). Upon
        error, do not attempt to retry sending the record immediately -- the
        return value indicates that the complete iteration over this channel's
        data should be repeated (see handleChannel()).

        Params:
            client = dht client instance to send data to other nodes
            key = record key
            value = record value
            channel = storage channel to which the record belongs

        Returns:
            true if the record was successfully forwarded or false if an error
            occurred

    ***************************************************************************/

    private bool sendRecord ( DhtClient client, char[] key, char[] value,
        KVStorageEngine channel )
    {
        bool error;

        char[] put_dg ( client.RequestContext )
        {
            return value;
        }

        void notifier ( client.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                log.error("Error while sending record {} to channel '{}': {}",
                    key, channel.id, info.message(client.msg_buf));
                error = true;
            }
        }

        auto hash = Hash.straightToHash(key);
        client.perform(this.reader.fiber, client.put(channel.id, hash, &put_dg,
            &notifier));

        return !error;
    }


    /***************************************************************************

        Advances the provided iterator to the next record, removing the current
        record from the storage engine if required.

        Note that the removal of a record is performed *after* the iterator has
        been advanced. This is necessary in order to keep the iteration
        consistent.

        Params:
            iterator = iterator over current storage engine
            remove_record = indicates that the record pointed at by the
                iterator should be removed after iteration
            channel = storage engine being iterated

    ***************************************************************************/

    private void advanceIteration ( IStepIterator iterator,
        bool remove_record, KVStorageEngine channel )
    {
        if ( remove_record )
        {
            (*this.resources.key_buffer).copy(iterator.key);
        }

        iterator.next();

        if ( remove_record )
        {
            channel.remove(*this.resources.key_buffer);
        }
    }
}

