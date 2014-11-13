/*******************************************************************************

    Redistribute request class.

    copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

    A Redistribute request instructs the node to change its hash responsibility
    range and to forward any records for which it is no longer responsible to
    another node. The client sending this request is required to include a list
    of replacement nodes, along with their hash responsibility ranges.

*******************************************************************************/

module swarmnodes.common.kvstore.request.RedistributeRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.common.kvstore.request.model.IRequest;

private import swarmnodes.common.kvstore.request.params.RedistributeNode;

private import swarmnodes.common.kvstore.connection.DhtClient;

private import swarmnodes.common.kvstore.storage.IStepIterator;

private import Hash = swarm.core.Hash;

private import swarm.dht.DhtConst : HashRange;

private import swarm.dht.client.connection.model.IDhtNodeConnectionPoolInfo;

private import ocean.core.Array : copy;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("swarmnodes.common.kvstore.request.RedistributeRequest");
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

        Code indicating the result of handling a record. See handleRecord().

    ***************************************************************************/

    private enum HandleRecordResult
    {
        Kept,
        Forwarded,
        Error
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

        ulong num_records_before = channel.num_records;

        this.resources.iterator.setStorage(channel);

        bool error_during_iteration;
        do
        {
            error_during_iteration = false;
            ulong num_records_iterated;

            channel.getAll(this.resources.iterator);

            while ( !this.resources.iterator.lastKey )
            {
                auto action = this.handleRecord(client, channel.id,
                    this.resources.iterator.key, this.resources.iterator.value);

                bool forwarded;
                with ( HandleRecordResult ) switch ( action )
                {
                    case Forwarded:
                        forwarded = true;
                        break;
                    case Error:
                        error_during_iteration = true;
                    default:
                        break;
                }

                this.advanceIteration(this.resources.iterator, forwarded, channel);
                num_records_iterated++;
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
        while ( error_during_iteration )
    }


    /***************************************************************************

        Advances the provided iterator to the next record, removing the current
        record from the storage engine if it has just been forwarded to another
        node.

        Note that the removal of a record is performed *after* the iterator has
        been advanced. This is necessary in order to keep the iteration
        consistent.

        Params:
            iterator = iterator over current storage engine
            current_forwarded = indicates whether the record pointed at by the
                iterator has just been forwarded to another node
            channel = storage engine being iterated

    ***************************************************************************/

    private void advanceIteration ( IStepIterator iterator,
        bool current_forwarded, KVStorageEngine channel )
    {
        if ( current_forwarded )
        {
            (*this.resources.key_buffer).copy(iterator.key);
        }

        iterator.next();

        if ( current_forwarded )
        {
            channel.remove(*this.resources.key_buffer);
        }
    }


    /***************************************************************************

        Works out whether the given record needs to be relocated or whether this
        node is still responsible for it. In the latter case, nothing is done.
        If the record needs to be relocated, then it is forwarded to the
        appropriate node.

        Params:
            client = dht client instance to send data to other nodes
            channel = name of storage channel to which the record belongs
            key = record key
            value = record value

        Returns:
            a code indicating whether the record was kept in this node,
            forwarded to another node, or whether an error occurred during
            forwarding

    ***************************************************************************/

    private HandleRecordResult handleRecord ( DhtClient client, char[] channel,
        char[] key, char[] value )
    {
        auto hash = Hash.straightToHash(key);
        foreach ( node; client.nodes )
        {
            auto dht_node = cast(IDhtNodeConnectionPoolInfo)node;
            if ( Hash.isWithinNodeResponsibility(
                hash, dht_node.min_hash, dht_node.max_hash) )
            {
                return this.forwardRecord(client, channel, hash, value)
                    ? HandleRecordResult.Forwarded : HandleRecordResult.Error;
            }
        }

        return HandleRecordResult.Kept;
    }


    /***************************************************************************

        Forwards the specified record to the node which is now responsible for
        it.

        Upon error, do not attempt to retry sending the record immediately.
        The return value indicates that it should not be removed from the
        storage engine, allowing resending to be retried later.

        Params:
            client = dht client instance to send data to other nodes
            channel = name of storage channel to which the record belongs
            hash = hash of record key
            value = record value

        Returns:
            true if the record was successfully forwarded or false if an error
            occurred

    ***************************************************************************/

    private bool forwardRecord ( DhtClient client, char[] channel, hash_t hash,
        char[] value )
    {
        char[] put_dg ( client.RequestContext )
        {
            return value;
        }

        bool error;

        void notifier ( client.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                log.error("Error while sending record '{}'/0x{:x16}: {}",
                    channel, hash, info.message(client.msg_buf));
                error = true;
            }
        }

        client.perform(this.reader.fiber, client.put(
            channel, hash, &put_dg, &notifier));

        return !error;
    }
}

