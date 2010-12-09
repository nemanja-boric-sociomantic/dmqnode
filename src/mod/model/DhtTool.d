/*******************************************************************************

    DHT node tool abstract class
    
    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        December 2010: Initial release
    
    authors:        Gavin Norman

    Base class for DHT tools.
    
    Provides the following command line parameters:
        -h = display help
        -S = dhtnodes.xml source file
        -k = process just a single record with the specified key (hash)
        -s = start of range to process (hash value - defaults to 0x00000000)
        -e = end of range to process (hash value - defaults to 0xFFFFFFFF)
        -C = process complete hash range
        -c = channel name to process
        -A = process all channels

*******************************************************************************/

module src.mod.model.DhtTool;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.core.Array;

private import ocean.text.Arguments;

private import swarm.dht.DhtClient,
               swarm.dht.DhtHash,
               swarm.dht.DhtConst;

private import swarm.dht.client.connection.ErrorInfo;

private import swarm.dht.client.DhtNodesConfig;

private import tango.io.Stdout;



/*******************************************************************************

    Dht tool abstract class

*******************************************************************************/

abstract class DhtTool
{
    /***************************************************************************

        Name of xml file which includes the dht node config

    ***************************************************************************/

    protected char[] xml;


    /***************************************************************************

        Flag indicating whether a dht error has occurred
    
    ***************************************************************************/

    protected bool dht_error;


    /***************************************************************************

        Query key range struct
    
    ***************************************************************************/

    struct Range
    {
        enum RangeType
        {
            SingleKey,
            KeyRange,
            CompleteRange
        }

        RangeType type;
        
        hash_t key1, key2;
    }

    protected Range range;


    /***************************************************************************

        Query channels struct
    
    ***************************************************************************/

    struct Channels
    {
        bool all_channels;
        
        char[] channel;
    }

    protected Channels channels;


    /***************************************************************************

        TODO

    ***************************************************************************/

    protected void process ( Arguments args )
    {
        auto dht = this.init(args);

        if ( this.channels.all_channels )
        {
            with ( this.range.RangeType ) switch ( this.range.type )
            {
                case KeyRange:
                case CompleteRange:
                    this.processAllChannels(dht, this.range.key1, this.range.key2);
                    break;
            }
        }
        else
        {
            with ( this.range.RangeType ) switch ( this.range.type )
            {
                case SingleKey:
                    this.processRecord(dht, this.channels.channel, this.range.key1);
                    break;

                case KeyRange:
                case CompleteRange:
                    this.processChannel(dht, this.channels.channel, this.range.key1, this.range.key2);
                    break;
            }
        }

        this.finished(dht);
    }


    /***************************************************************************

        TODO
    
    ***************************************************************************/

    private void processAllChannels ( DhtClient dht, hash_t start, hash_t end )
    {
        char[][] channels;
        dht.getChannels(
                ( uint id, char[] channel )
                {
                    if ( channel.length )
                    {
                        channels.appendCopy(channel);
                    }
                }
            ).eventLoop();

        foreach ( channel; channels )
        {
            this.processChannel(dht, channel, start, end);
        }
    }
    

    /***************************************************************************

        TODO
    
    ***************************************************************************/

    abstract protected void processChannel ( DhtClient dht, char[] channel, hash_t start, hash_t end );


    /***************************************************************************

        TODO
    
    ***************************************************************************/

    abstract protected void processRecord ( DhtClient dht, char[] channel, hash_t key);


    /***************************************************************************

        TODO
    
    ***************************************************************************/

    protected void finished ( DhtClient dht )
    {
    }


    /***************************************************************************

        TODO
    
    ***************************************************************************/

    private void addArgs ( Arguments args )
    {
        args("help").aliased('?').aliased('h').help("display this help");
        args("source").params(1).required().aliased('S').help("path of dhtnodes.xml file defining nodes to dump");
        args("key").params(1).aliased('k').help("fetch just a single record with the specified key (hash)");
        args("start").params(1).aliased('s').help("start of range to query (hash value - defaults to 0x00000000)");
        args("end").params(1).aliased('e').help("end of range to query (hash value - defaults to 0xFFFFFFFF)");
        args("complete_range").aliased('C').help("fetch records in the complete hash range");
        args("channel").conflicts("all_channels").params(1).aliased('c').help("channel name to query");
        args("all_channels").conflicts("channel").aliased('A').help("query all channels");

        this.addArgs_(args);
    }
    

    /***************************************************************************

        TODO
    
    ***************************************************************************/

    protected void addArgs_ ( Arguments args )
    {
    }


    /***************************************************************************

        TODO
    
    ***************************************************************************/

    protected bool validArgs ( Arguments args, char[][] arguments )
    {
        this.addArgs(args);

        if ( !args.parse(arguments) )
        {
            Stderr.formatln("Invalid arguments");
            return false;
        }

        if ( !this.validArgs_(args) )
        {
            return false;
        }

        if ( !args.exists("source") )
        {
            Stderr.formatln("No xml source file specified (use -S)");
            return false;
        }
        
        bool all_channels = args.exists("all_channels");
        bool one_channel = args.exists("channel");

        if ( !oneTrue(all_channels, one_channel) )
        {
            Stderr.formatln("Please specify exactly one of the following options: single channel (-c) or all channels (-A)");
            return false;
        }

        bool complete_range = args.exists("complete_range");
        bool key_range = args.exists("start") || args.exists("end");
        bool single_key = args.exists("key");

        if ( !oneTrue(complete_range, key_range, single_key) )
        {
            Stderr.formatln("Please specify exactly one of the following options: complete range (-C), key range (-s .. -e) or single key (-k)");
            return false;
        }
        
        if ( single_key && all_channels )
        {
            Stderr.formatln("Cannot process a single key (-k) over all channels (-A)");
            return false;
        }

        return true;
    }


    /***************************************************************************

        TODO
    
    ***************************************************************************/

    protected bool validArgs_ ( Arguments args )
    {
        return true;
    }


    /***************************************************************************

        TODO
    
    ***************************************************************************/

    protected void init_ ( Arguments args )
    {
    }


    /***************************************************************************

        TODO
    
    ***************************************************************************/

    private DhtClient init ( Arguments args )
    {
        this.xml = args.getString("source");
        
        if ( args.exists("all_channels") )
        {
            this.channels.all_channels = true;
            this.channels.channel.length = 0;
        }
        else if ( args.exists("channel") )
        {
            this.channels.all_channels = false;
            this.channels.channel = args.getString("channel");
        }

        if ( args.exists("complete_range") )
        {
            this.range.type = this.range.type.CompleteRange;
            this.range.key1 = 0x00000000;
            this.range.key2 = 0xffffffff;
        }
        else if ( args.exists("start") || args.exists("end") )
        {
            this.range.type = this.range.type.KeyRange;
            this.range.key1 = args.exists("start") ? args.getInt!(hash_t)("start") : 0x00000000;
            this.range.key2 = args.exists("end") ? args.getInt!(hash_t)("end") : 0xffffffff;
        }
        else if ( args.exists("key") )
        {
            this.range.type = this.range.type.SingleKey;
            this.range.key1 = args.getInt!(hash_t)("key");
            this.range.key2 = this.range.key1;
        }

        this.init_(args);

        return this.initDhtClient(this.xml);
    }


    /***************************************************************************

        Initialises a dht client, adding nodes from a config file in
        etc/dhtnodes.xml, and querying the node ranges.
    
        Returns:
            initialised dht client
    
        Throws:
            asserts that no errors occurred during initialisation
    
    ***************************************************************************/

    private DhtClient initDhtClient ( char[] xml )
    {
        auto dht = new DhtClient();
        
        dht.error_callback(&this.dhtError);
        
        DhtNodesConfig.addNodesToClient(dht, xml);
        dht.nodeHandshake();
        assert(!this.dht_error);
    
        return dht;
    }


    /***************************************************************************

        TODO
    
    ***************************************************************************/

    private void dhtError ( ErrorInfo e )
    {
        Stderr.format("DHT client error: {}\n", e.message);
        this.dht_error = true;
    }


    /***************************************************************************

        TODO
    
    ***************************************************************************/

    static bool oneTrue ( bool[] bools ... )
    {
        uint true_count;
        foreach ( b; bools )
        {
            if ( b ) true_count++;
        }

        return true_count == 1;
    }
}

