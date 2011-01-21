/*******************************************************************************

    DHT node tool abstract class - for tools which read from a source dht

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman

    Base class for DHT tools which connect to a node cluster specified in an xml
    file, and provide commands over single keys, key sub-ranges and the complete
    hash range, and over a specified channel or all channels.

    Provides the following command line parameters:
        -S = dhtnodes.xml source file
        -k = process just a single record with the specified key (hash)
        -s = start of range to process (hash value - defaults to 0x00000000)
        -e = end of range to process (hash value - defaults to 0xFFFFFFFF)
        -C = process complete hash range
        -c = channel name to process
        -A = process all channels

    Inherited from super class:
        -h = display help

*******************************************************************************/

module mod.model.SourceDhtTool;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.model.DhtTool;

private import ocean.core.Array;

private import ocean.text.Arguments;

version ( NewDhtClient )
{
    private import swarm.dht2.DhtClient,
                   swarm.dht2.DhtHash,
                   swarm.dht2.DhtConst;
}
else
{
    private import swarm.dht.DhtClient,
                   swarm.dht.DhtHash,
                   swarm.dht.DhtConst;
}

private import tango.core.Array;

private import tango.io.Stdout;



/*******************************************************************************

    Dht source tool abstract class

*******************************************************************************/

abstract class SourceDhtTool : DhtTool
{
    /***************************************************************************

        Query key range struct
    
    ***************************************************************************/

    struct Range
    {
        enum RangeType
        {
            SingleKey,
            KeyRange
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

        Main process method. Runs the tool based on the passed command line
        arguments.
    
        Params:
            dht = dht client to use

    ***************************************************************************/
    
    protected void process_ ( DhtClient dht )
    {
        if ( this.channels.all_channels )
        {
            with ( this.range.RangeType ) switch ( this.range.type )
            {
                case KeyRange:
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
                    this.processChannel(dht, this.channels.channel, this.range.key1, this.range.key2);
                    break;
            }
        }
    
        this.finished(dht);
    }


    /***************************************************************************

        Runs the tool over the specified hash range on a single channel.

        Params:
            dht = dht client
            channel = name of channel
            start = start of hash range
            end = end of hash range
    
    ***************************************************************************/

    abstract protected void processChannel ( DhtClient dht, char[] channel, hash_t start, hash_t end );


    /***************************************************************************

        Runs the tool over the specified record in a single channel.

        Params:
            dht = dht client
            channel = name of channel
            key = record hash
    
    ***************************************************************************/

    abstract protected void processRecord ( DhtClient dht, char[] channel, hash_t key);


    /***************************************************************************

        Sets up the list of handled command line arguments. This method sets up
        only the base class' arguments (see module header), then calls the
        addArgs__() method to set up any additional command line arguments
        required by the derived class.

        Params:
            args = arguments object
    
    ***************************************************************************/
    
    final override protected void addArgs_ ( Arguments args )
    {
        args("source").params(1).required().aliased('S').help("path of dhtnodes.xml file defining nodes to dump");
        args("key").params(1).aliased('k').help("fetch just a single record with the specified key (hash)");
        args("start").params(1).aliased('s').help("start of range to query (hash value - defaults to 0x00000000)");
        args("end").params(1).aliased('e').help("end of range to query (hash value - defaults to 0xFFFFFFFF)");
        args("complete_range").aliased('C').help("fetch records in the complete hash range");
        args("channel").conflicts("all_channels").params(1).aliased('c').help("channel name to query");
        args("all_channels").conflicts("channel").aliased('A').help("query all channels");

        this.addArgs__(args);
    }

    protected void addArgs__ ( Arguments args )
    {
    }


    /***************************************************************************

        Validates command line arguments in the passed Arguments object. This
        method validates only the base class' arguments (see module header),
        then calls the validArgs_() method to validate any additional command
        line arguments required by the derived class.
    
        Params:
            args = arguments object used to parse command line arguments
    
        Returns:
            true if the command line args are valid
    
    ***************************************************************************/
    
    final override protected bool validArgs ( Arguments args )
    {
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
    
        return this.validArgs_(args);
    }

    protected bool validArgs_ ( Arguments args )
    {
        return true;
    }


    /***************************************************************************

        Reads the tool's settings from validated command line arguments. This
        method reads only the base class' arguments (see module header), then
        calls the readArgs_() method to read any additional command line
        arguments required by the derived class.

        Params:
            args = arguments object to read
    
    ***************************************************************************/

    final override protected void readArgs ( Arguments args )
    {
        super.dht_nodes_config = args.getString("source");
        
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
            this.range.type = this.range.type.KeyRange;
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

        this.readArgs_(args);
    }

    protected void readArgs_ ( Arguments args )
    {
    }


    /***************************************************************************

        Runs the tool over the specified hash range on all channels in the
        dht node cluster. The channels are processed in series.

        Params:
            dht = dht client
            start = start of hash range
            end = end of hash range

    ***************************************************************************/

    private void processAllChannels ( DhtClient dht, hash_t start, hash_t end )
    {
        char[][] channels;
        dht.getChannels(
                ( uint id, char[] channel )
                {
                    if ( channel.length && !channels.contains(channel) )
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
}

