/*******************************************************************************

    DHT node copy

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        October 2010: Initial release
                    February 2011: Re-written using SourceDhtTool base class

    authors:        Gavin Norman

    Copies data from a source dht to the destination dht.

    Can also be used (with the -X flag) to compare the contents of two dhts.

    Command line parameters:
        -D = dhtnodes.xml file for destination dht
        -X = compare specified data in source & destination dhts (do not copy)
        -t = type of dht (memory / logfiles)

    Inherited from super class:
        -h = display help
        -S = dhtnodes.xml source file
        -t = type of dht (memory / logfiles)
        -k = copy just a single record with the specified key (hash)
        -s = start of range to copy (hash value - defaults to 0x00000000)
        -e = end of range to copy   (hash value - defaults to 0xFFFFFFFF)
        -C = copy complete hash range (0x00000000 .. 0xFFFFFFFF)
        -c = channel name to copy
        -A = copy all channels

 ******************************************************************************/

module src.mod.copy.DhtCopy;



/*******************************************************************************

    Imports

 ******************************************************************************/

private import src.mod.model.SourceDhtTool;

private import ocean.core.Array;

private import ocean.text.Arguments;

private import ocean.util.log.PeriodicTrace;

private import swarm.core.client.helper.SuspendableThrottler;

private import swarm.dht.DhtClient,
               swarm.dht.DhtHash,
               swarm.dht.DhtConst;

private import swarm.dht.node.storage.filesystem.LogRecord;

private import ocean.io.Stdout;



/*******************************************************************************

    Dht copy tool

*******************************************************************************/

public class DhtCopy : SourceDhtTool
{
    /***************************************************************************

        Xml nodes config file for destination dht
    
    ***************************************************************************/

    private char[] dst_config;


    /***************************************************************************

        Source & destination dht clients

    ***************************************************************************/

    private alias dht src_dht; // super.dht

    private DhtClient dst_dht;


    /***************************************************************************

        Pool of records being written to the dht.

    ***************************************************************************/

    private SuspendableThrottlerStringPool put_pool;


    /***************************************************************************

        Maximum number of pending records per node in the source dht before the
        input is suspended. Input is resumed when the size of the pending pool
        reduces once more.

    ***************************************************************************/

    private const per_connection_suspend_point = 5_000;


    /***************************************************************************

        Toggles compare vs copy

    ***************************************************************************/

    private bool compare;


    /***************************************************************************

        In compare mode, count of non-matching records
    
    ***************************************************************************/

    private ulong non_compare_count;


    /***************************************************************************

        Flag indicating whether the dhts being copied from/to are memory dhts.
        (true = memory, false = logfiles)

    ***************************************************************************/

    private bool memory;


    /***************************************************************************

        Set to true when not copying a complete channel (i.e. rather copying a
        sub-range of a channel).

    ***************************************************************************/

    private bool not_copying_all;


    /***************************************************************************

        When copying a complete channel, a GetChannelSize request is performed
        to find the total number of records in the channel. This member stores
        the results of the request, and is used to display a percentage progress
        output.

    ***************************************************************************/

    private ulong records_in_src;


    /***************************************************************************

        Count of records processed.

    ***************************************************************************/

    private ulong processed;


    /***************************************************************************

        Channel being processed (set by the processChannel() method).

    ***************************************************************************/

    private char[] channel;


    /***************************************************************************
    
        Adds command line arguments specific to this tool.
        
        Params:
            args = command line arguments object to add to
    
    ***************************************************************************/
    
    override protected void addArgs__ ( Arguments args )
    {
        args("type").params(1).required.aliased('t').restrict(["memory", "logfiles"]).help("type of dht (memory / logfiles");
        args("dest").params(1).required.aliased('D').help("path of dhtnodes.xml file defining nodes to import records to");
        args("compare").aliased('X').help("compare specified data in source & destination dhts (do not copy)");
    }


    /***************************************************************************
    
        Checks whether the parsed command line args are valid.
    
        Params:
            args = command line arguments object to validate
    
        Returns:
            true if args are valid
    
    ***************************************************************************/
    
    override protected bool validArgs_ ( Arguments args )
    {
        auto type = args.getString("type");

        if ( type == "logfiles" && args.getBool("compare") == true )
        {
            Stderr.formatln("Compare mode doesn't work with logfiles dhts");
            return false;
        }

        return true;
    }


    /***************************************************************************
    
        Initialises this instance from the specified command line args.
    
        Params:
            args = command line arguments object to read settings from
    
    ***************************************************************************/

    override protected void readArgs__ ( Arguments args )
    {
        this.dst_config = args.getString("dest");

        this.compare = args.getBool("compare");

        this.memory = args.getString("type") == "memory";
    }


    /***************************************************************************
    
        Initialises the destination dht client and the record pool.
    
        Params:
            args = command line arguments object to read settings from
    
    ***************************************************************************/

    private void initDst ( )
    {
        this.processed = 0;
        this.non_compare_count = 0;

        if ( this.dst_dht is null )
        {
            this.dst_dht = super.initDhtClient(this.dst_config, 1_000_000);
        }

        if ( this.put_pool is null )
        {
            this.put_pool = new SuspendableThrottlerStringPool(
                    this.src_dht.registry.length * this.per_connection_suspend_point);
        }
    }


    /***************************************************************************

        Copies dht records in the specified hash range in the specified
        channel to the destination dht.

        Params:
            src_dht = dht client to perform copy from
            channel = name of channel to copy from
            start = start of hash range to copy
            end = end of hash range to copy

    ***************************************************************************/

    protected void processChannel ( char[] channel, hash_t start, hash_t end )
    {
        void getDg ( DhtClient.RequestContext c, char[] key, char[] value )
        {
            if ( value.length )
            {
                auto hash = DhtHash.straightToHash(key);

                if ( hash >= start && hash <= end )
                {
                    this.processed++;
                    this.handleRecord(channel, hash, value);
                }

                this.progressDisplay();
            }
        }

        this.channel = channel;

        if ( start > hash_t.min || end < hash_t.max )
        {
            not_copying_all = true;
        }
        else
        {
            void channelSizeCb ( DhtClient.RequestContext c, char[] address, ushort port, char[] channel, ulong records, ulong bytes )
            {
                this.records_in_src += records;
            }

            this.records_in_src = 0;
            src_dht.assign(src_dht.getChannelSize(channel, &channelSizeCb, &super.notifier));
            super.epoll.eventLoop;
        }

        this.initDst();

        if ( this.memory )
        {
            src_dht.assign(src_dht.getAll(channel, &getDg, &super.notifier).raw
                    .suspendable(&this.put_pool.suspender));
        }
        else
        {
            hash_t bucket_mask = (1 << LogRecord.SplitBits.key_bits) - 1;

            Stdout.formatln("Channel '{}', start = {:x}, end = {:x}", channel, start, end);

            assert((start & bucket_mask) == 0, "I don't do that sort of thing: bad start");
            assert((end & bucket_mask) == bucket_mask, "I don't do that sort of thing: bad end");

            src_dht.assign(src_dht.getRange(channel, start, end, &getDg, &super.notifier)
                    .raw.suspendable(&this.put_pool.suspender));
        }

        super.epoll.eventLoop;

        this.finishedOutput(channel);
    }


    /***************************************************************************

        Copies a single dht record with the specified hash in the specified
        channel to the destination dht.

        Params:
            src_dht = dht client to perform copy from
            channel = channel to copy from
            key = hash of record to copy

    ***************************************************************************/

    protected void processRecord ( char[] channel, hash_t key )
    {
        void getDg ( DhtClient.RequestContext c, char[] value )
        {
            if ( value.length )
            {
                this.processed++;

                this.handleRecord(channel, key, value);
            }
        }

        this.initDst();

        src_dht.assign(src_dht.get(channel, key, &getDg, &super.notifier).raw);
        this.epoll.eventLoop;

        this.finishedOutput(channel);
    }


    /***************************************************************************
    
        Outputs a progress display when copying / comparing a channel.
        
        Params:
            processed = count of processed records
            received = count of received records
            not_copying_all = if true, indicates that received may be >
                processed

    ***************************************************************************/

    private void progressDisplay ( )
    {
        if ( this.not_copying_all )
        {
            if ( this.compare )
            {
                StaticPeriodicTrace.format("{}: {} compared, {} pending, {} non-matching",
                        this.channel, this.processed, this.put_pool.length, this.non_compare_count);
            }
            else
            {
                StaticPeriodicTrace.format("{}: {} copied, {} pending",
                        this.channel, this.processed, this.put_pool.length);
            }
        }
        else
        {
            auto percent = (cast(double)processed / cast(double)this.records_in_src) * 100.0;

            if ( this.compare )
            {
                StaticPeriodicTrace.format("{}: {} / {} compared ({}%), {} pending, {} non-matching",
                        this.channel, this.processed, this.records_in_src, percent,
                        this.put_pool.length, this.non_compare_count);
            }
            else
            {
                StaticPeriodicTrace.format("{}: {} / {} copied ({}%), {} pending",
                        this.channel, this.processed, this.records_in_src, percent,
                        this.put_pool.length);
            }
        }
    }


    /***************************************************************************

        Outputs a final message when all records have been copied.

        Params:
            channel = channel copied / compared from

    ***************************************************************************/

    private void finishedOutput ( char[] channel )
    {
        if ( this.compare )
        {
            Stdout.formatln("{}: {} compared, {} non-matching",
                    channel, this.processed, this.non_compare_count).clearline;
        }
        else
        {
            Stdout.formatln("{}: copied {}", channel, this.processed).clearline;
        }
    }


    /***************************************************************************

        Copies / compares a single dht record.

        Params:
            channel = channel to copy / compare from
            key = hash of record to copy / compare
            value = value of record to copy / compare

    ***************************************************************************/

    private void handleRecord ( char[] channel, hash_t key, char[] value )
    {
        auto context = this.put_pool.put(value);

        if ( this.compare )
        {
            this.dst_dht.assign(this.dst_dht.get(channel, key, &this.compareGetDg,
                    &handleNotifier).raw.context(context));
        }
        else
        {
            if ( memory )
            {
                this.dst_dht.assign(this.dst_dht.put(channel, key, &this.putDg,
                        &handleNotifier).context(context));
            }
            else
            {
                this.dst_dht.assign(this.dst_dht.putDup(channel, key, &this.putDg,
                        &handleNotifier).context(context));
            }
        }
    }


    /***************************************************************************

        Callback for Get requests in compare mode. Compares the record fetched
        from the destination dht with the record from the source dht.

        Params:
            c = request context (contains a reference to an item in the pool of
                pending items)
            dst_value = record value read from destination dht, should
                correspond to the record referred to by c

    ***************************************************************************/

    private void compareGetDg ( DhtClient.RequestContext c, char[] dst_value )
    {
        if ( dst_value != this.put_pool.get(c) )
        {
            this.non_compare_count++;
        }
    }


    /***************************************************************************

        Callback for Put / PutDup requests.

        Params:
            c = request context (contains a reference to an item in the pool of
                pending items)

        Returns:
            record to put to dht

    ***************************************************************************/

    private char[] putDg ( DhtClient.RequestContext c )
    {
        return this.put_pool.get(c);
    }


    /***************************************************************************

        Notification callback used by handleRecord() method, above. Updates the
        progress display, and recycles a pending item into the pool when it has
        finished.

        Params:
            info = notification info struct

    ***************************************************************************/

    private void handleNotifier ( DhtClient.RequestNotification info )
    {
        if ( info.type == info.type.Finished )
        {
            if ( this.compare && !info.succeeded )
            {
                this.non_compare_count++;
            }

            this.progressDisplay();

            this.put_pool.finished(info.context);
        }

        super.notifier(info);
    }
}

