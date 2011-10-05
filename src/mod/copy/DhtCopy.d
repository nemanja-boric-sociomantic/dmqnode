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

    Inherited from super class:
        -h = display help
        -S = dhtnodes.xml source file
        -k = copy just a single record with the specified key (hash)
        -s = start of range to copy (hash value - defaults to 0x00000000)
        -e = end of range to copy   (hash value - defaults to 0xFFFFFFFF)
        -C = copy complete hash range (0x00000000 .. 0xFFFFFFFF)
        -c = channel name to copy
        -A = copy all channels

 ******************************************************************************/

module mod.copy.DhtCopy;



/*******************************************************************************

    Imports

 ******************************************************************************/

private import src.mod.model.SourceDhtTool;

private import core.dht.DestinationQueue;

private import ocean.core.Array;

private import ocean.text.Arguments;

private import ocean.util.log.PeriodicTrace;

private import swarm.dht.DhtClient,
               swarm.dht.DhtHash,
               swarm.dht.DhtConst;

private import tango.io.Stdout;



/*******************************************************************************

    Dht copy tool

*******************************************************************************/

class DhtCopy : SourceDhtTool
{
    /***************************************************************************
    
        Singleton parseArgs() and run() methods.
    
    ***************************************************************************/
    
    mixin SingletonMethods;


    /***************************************************************************

        Xml nodes config file for destination dht
    
    ***************************************************************************/

    private char[] dst_config;


    /***************************************************************************

        Destination dht
    
    ***************************************************************************/

    private DhtClient dst_dht;


    /***************************************************************************

        Queue of records being pushed in batches to the dht.
    
    ***************************************************************************/
    
    private DestinationQueue put_queue;

    
    /***************************************************************************

        Toggles compare vs copy

    ***************************************************************************/

    private bool compare;


    /***************************************************************************

        In compare mode, count of non-matching records
    
    ***************************************************************************/

    private ulong non_compare_count;


    /***************************************************************************
    
        Adds command line arguments specific to this tool.
        
        Params:
            args = command line arguments object to add to
    
    ***************************************************************************/
    
    override protected void addArgs__ ( Arguments args )
    {
        args("dest").params(1).required().aliased('D').help("path of dhtnodes.xml file defining nodes to import records to");
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
        if ( !args.exists("dest") )
        {
            Stderr.formatln("No xml destination file specified (use -D)");
            return false;
        }

        return true;
    }
    
    
    /***************************************************************************
    
        Initialises this instance from the specified command line args.
    
        Params:
            args = command line arguments object to read settings from
    
    ***************************************************************************/
    
    override protected void readArgs_ ( Arguments args )
    {
        this.dst_config = args.getString("dest");

        this.compare = args.getBool("compare");

        this.dst_dht = super.initDhtClient(this.dst_config);
        this.put_queue = new DestinationQueue(this.dst_dht);
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
    
    protected void processChannel ( DhtClient src_dht, char[] channel, hash_t start, hash_t end )
    {
        bool not_copying_all;
        ulong received, processed;

        void getDg ( DhtClient.RequestContext c, char[] key, char[] value )
        {
            if ( value.length )
            {
                received++;

                auto hash = DhtHash.straightToHash(key);

                if ( hash >= start && hash <= end )
                {
                    this.handleRecord(channel, hash, value);
                    processed++;
                }

                this.progressDisplay(channel, processed, received, not_copying_all);
            }
        }

        this.put_queue.setChannel(channel);

        if ( src_dht.commandSupported(DhtConst.Command.GetRange) )
        {
            src_dht.getRangeRaw(channel, start, end, &getDg).eventLoop;
        }
        else
        {
            if ( start > hash_t.min || end < hash_t.max )
            {
                not_copying_all = true;
            }
            src_dht.getAllRaw(channel, &getDg).eventLoop;
        }

        this.put_queue.flush();

        if ( this.compare )
        {
            Stdout.formatln("Compared {} records from channel {}, {} didn't match", channel, processed, this.non_compare_count);
        }
        else
        {
            Stdout.formatln("Copied {} records from channel {}", channel, processed);
        }
    }


    /***************************************************************************
    
        Copies a single dht record with the specified hash in the specified
        channel to the destination dht.
        
        Params:
            src_dht = dht client to perform copy from
            channel = channel to copy from
            key = hash of record to copy
    
    ***************************************************************************/
    
    protected void processRecord ( DhtClient src_dht, char[] channel, hash_t key )
    {
        void getDg ( DhtClient.RequestContext c, char[] value )
        {
            if ( value.length )
            {
                this.handleRecord(channel, key, value);
            }
        }

        this.put_queue.setChannel(channel);

        src_dht.getRaw(channel, key, &getDg).eventLoop;

        this.put_queue.flush();
    }


    /***************************************************************************
    
        Outputs a progress display when copying / comparing a channel.
        
        Params:
            channel = channel to copy from
            processed = count of processed records
            received = count of received records
            not_copying_all = if true, indicates that received may be >
                processed
    
    ***************************************************************************/

    private void progressDisplay ( char[] channel, ulong processed, ulong received, bool not_copying_all )
    {
        if ( this.compare )
        {
            if ( not_copying_all )
            {
                StaticPeriodicTrace.format("{}: compared {} / {}, {} non-matching", channel, processed, received, this.non_compare_count);
            }
            else
            {
                StaticPeriodicTrace.format("{}: compared {}, {} non-matching", channel, processed, this.non_compare_count);
            }
        }
        else
        {
            if ( not_copying_all )
            {
                StaticPeriodicTrace.format("{}: copied {} / {}", channel, processed, received);
            }
            else
            {
                StaticPeriodicTrace.format("{}: copied {}", channel, processed);
            }
        }
    }


    /***************************************************************************
    
        Copies / comapres a single dht record.
        
        Params:
            channel = channel to copy / compare from
            key = hash of record to copy / compare
            value = value of record to copy / compare

    ***************************************************************************/

    private void handleRecord ( char[] channel, hash_t key, char[] value )
    {
        if ( this.compare )
        {
            void getDg ( DhtClient.RequestContext c, char[] dst_value )
            {
                if ( value != dst_value )
                {
                    this.non_compare_count++;
                }
            }

            this.dst_dht.getRaw(channel, key, &getDg).eventLoop;
        }
        else
        {
            this.put_queue.put(key, value);
        }
    }
}

