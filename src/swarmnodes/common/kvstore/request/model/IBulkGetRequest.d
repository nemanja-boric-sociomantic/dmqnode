/*******************************************************************************

    Abstract base class for key/value node requests which send a sequence of
    keys or key/value pairs to the client. (For example: GetRange, GetAll,
    GetAllKeys.)

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman

    During all iteration requests, this base class keeps track of the number of
    records scanned which were skipped (either due to being filtered out, or due
    to being not within the specified hash range, for example). If a certain
    number of records is skipped (i.e. not sent to the client) then the request
    manually returns to the epoll event loop (by using a custon event) which
    gives any other requests which might be running a chance to do something. In
    this way the iteration commands are prevented from blocking the node.

*******************************************************************************/

module swarmnodes.common.kvstore.request.model.IBulkGetRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.common.request.helper.LoopCeder;

private import swarm.dht.common.RecordBatcher;

private import swarmnodes.common.kvstore.request.model.IChannelRequest;

private import swarmnodes.common.kvstore.storage.IStepIterator;

private import ocean.io.select.client.FiberSelectEvent;

private import tango.util.log.Log;

// TODO: wile transitioning from the old to the new bulk get requests, all
// modules containing classes derived from IBulkGetRequest (i.e. all modules
// which import this module) need this import, hence making it public for
// convenience. When the old bulk get requests are removed, this import will no
// longer be required, and can also be removed.
public import swarm.dht.DhtConst;



/*******************************************************************************

    Static module logger

*******************************************************************************/

static private Logger log;
static this ( )
{
    log = Log.lookup("swarmnodes.common.kvstore.request.model.IBulkGetRequest");
}



/*******************************************************************************

    Bulk get request abstract base class

    Template Params:
        ChunkedBatcher = if true, the old lzo-chunked batcher is used to
        compress and transfer the record batches. Otherwise the batches are
        compressed and sent as simple (non-chunked) arrays.

        Note that the support for the two different types of batcher was
        implemented in this way (as opposed to, say, a base class with two
        derived classes -- one per type of batcher) in order to minimize the
        amount of code modification required to the end derived classes (GetAll,
        GetRange, etc).

*******************************************************************************/

public abstract scope class IBulkGetRequest ( bool ChunkedBatcher ) : IChannelRequest
{
    /***************************************************************************

        Aliases for the convenience of sub-classes, avoiding public imports.

    ***************************************************************************/

    protected alias .IStepIterator IStepIterator;

    protected alias .FiberSelectEvent FiberSelectEvent;

    protected alias RecordBatcher.AddResult AddResult;


    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( DhtConst.Command.E cmd, FiberSelectReader reader,
        FiberSelectWriter writer, IKVRequestResources resources )
    {
        super(cmd, reader, writer, resources);
    }


    /***************************************************************************

        Performs this request. (Fiber method, after command and channel validity
        have been confirmed.)

    ***************************************************************************/

    protected void handle___ ( )
    {
        this.writer.write(DhtConst.Status.E.Ok);

        auto storage_channel =
            *this.resources.channel_buffer in this.resources.storage_channels;
        if ( storage_channel !is null )
        {
            scope ( exit )
            {
                if ( this.resources.iterator )
                {
                    this.resources.iterator.finished();
                }
            }
            this.beginIteration(*storage_channel);

            do
            {
                this.writeBatch();
            }
            while ( !this.resources.iterator.lastKey );
        }

        this.writer.writeArray(""); // end of list
    }


    /***************************************************************************

        Compresses and sends a batch of records to the client.

    ***************************************************************************/

    protected void writeBatch ( )
    {
        static if ( ChunkedBatcher )
        {
            (*this.resources.batch_buffer).length = 0;
            this.getBatch();

            if ( (*this.resources.batch_buffer).length )
            {
                this.writer.writeChunkedArray(*this.resources.batch_buffer, true); // write as chunked lzo stream
            }
        }
        else
        {
            this.getBatch();

            auto compressed = this.resources.batcher.compress(
                *cast(ubyte[]*)this.resources.batch_buffer);
            this.writer.writeArray(compressed);
        }
    }


    /***************************************************************************

        Creates an iterator object and sets it to iterate over the selected
        channel. The iterator object only needs to be constructed once, the
        first time processing of a storage engine occurs.

        Note: It has to be done in this way, rather than initialising the
        iterator in this class' constructor, because in the constructor we have
        no idea what type the storage engine is, and so cannot create an
        iterator object for it. The type of the storage engine is only known at
        the point when it is used.

        Params:
            storage = storage channel to iterate over

    ***************************************************************************/

    private void beginIteration ( KVStorageEngine storage_channel )
    {
        this.resources.iterator.setStorage(storage_channel);

        this.beginIteration_(storage_channel, this.resources.iterator);
    }


    /***************************************************************************

        Initiates iteration over the specified channel using the specified
        iterator.

        Params:
            storage = storage channel to iterate over
            iterator = iterator instance to use

    ***************************************************************************/

    abstract protected void beginIteration_ ( KVStorageEngine storage_channel,
        IStepIterator iterator );


    /***************************************************************************

        Fills the internal batch buffer with as many records as will fit, as
        provided by the iterator. For each record which is iterated over, the
        decision of whether to include it in the batch or not is left up to the
        abstract addToBatch() method. After a number of records are processed
        (regardless of whether they are skipped or added to the batch), then the
        request cedes to epoll to avoid blocking the node.

        This method should presumably be called by derived classes at some point
        in their implementation of writeBatch().

    ***************************************************************************/

    protected void getBatch ( )
    {
        bool batch_full;
        AddResult add_result;
        uint processed_count;

        while ( !this.resources.iterator.lastKey && !batch_full )
        {
            auto record_handled = this.handleRecord(add_result);
            bool advance_iterator;

            if ( record_handled )
            {
                this.resources.node_info.handledRecord();

                with ( AddResult ) switch ( add_result )
                {
                    case Added:
                        advance_iterator = true;
                        batch_full = false;
                    break;
                    case BatchFull:
                        advance_iterator = false; // Send this record next time around
                        batch_full = true;
                    break;
                    case TooBig:
                        log.warn("Large record ({} bytes) being skipped in bulk "
                            "request on channel {} (key = {})",
                            this.resources.iterator.value.length,
                            *this.resources.channel_buffer,
                            this.resources.iterator.key);
                        advance_iterator = true;
                        batch_full = false;
                    break;
                    default:
                        assert(false, "Invalid AddResult in switch");
                }
            }
            else
            {
                advance_iterator = true;
                batch_full = false;
            }

            if ( advance_iterator )
            {
                this.resources.iterator.next();
            }

            this.resources.loop_ceder.handleCeding();

            /* TODO
            *
            * Set a max bandwidth for bulk commands (30Mb or something)
            * Then divide that total between all active bulk requests.
            * Using a timer and a counter, track the amount of data sent
            * by each bulk request per second, and insert pauses to bring
            * it down to the desired maximum level.
            *
            * There are two points here. Firstly there's the actual bandwidth
            * transmitted. Secondly there's the time spent scanning / building
            * up batches. The second issue is handled by the ceding above.
            *
            * The iteration bandwidth limit is also useful to cap the cpu usage,
            * which can get quite high when iterating.
            TODO */
        }
    }


    /***************************************************************************

        Returns:
            the current key

    ***************************************************************************/

    protected char[] key ( )
    {
        return this.resources.iterator.key;
    }


    /***************************************************************************

        Returns:
            the current value

    ***************************************************************************/

    protected char[] value ( )
    {
        return this.resources.iterator.value;
    }


    /***************************************************************************

        Called by getBatch() when a record is retrieved from the storage engine.
        Should add the record to the batch if desired (using the protected
        addToBatch() methods, below), and return true in this case.

        Params:
            add_result = out value which receives a code indicating if the
                record was successfully added, and if not, why not

        Returns:
            true if the record was added

    ***************************************************************************/

    abstract protected bool handleRecord ( out AddResult add_result );


    /***************************************************************************

        Adds a single value to the batch.

        Params:
            v = value to add to batch

        Returns:
            code indicating if the record was successfully added, and if not,
            why not

    ***************************************************************************/

    protected AddResult addToBatch ( char[] v )
    {
        static if ( ChunkedBatcher )
        {
            return RecordBatcher.add(*this.resources.batch_buffer, v);
        }
        else
        {
            return this.resources.batcher.add(v);
        }
    }


    /***************************************************************************

        Adds a pair of values to the batch.

        Params:
            v1 = first value to add to batch
            v2 = second value to add to batch

        Returns:
            code indicating if the pair was successfully added, and if not,
            why not

    ***************************************************************************/

    protected AddResult addToBatch ( char[] v1, char[] v2 )
    {
        static if ( ChunkedBatcher )
        {
            return RecordBatcher.add(*this.resources.batch_buffer, v1, v2);
        }
        else
        {
            return this.resources.batcher.add(v1, v2);
        }
    }
}

