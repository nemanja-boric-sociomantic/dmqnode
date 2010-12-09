/*******************************************************************************

    DHT node dump
    
    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        October 2010: Initial release
    
    authors:        Gavin Norman
    
    Reads records from one or more dht nodes and outputs them to stdout.
    
    Command line parameters:
        -n = count records, do not dump contents
        -x = displays records as hexadecimal dump (default is a string dump)
        -l = limits the length of text displayed for each record

    Inherited from super class:
        -h = display help
        -S = dhtnodes.xml source file
        -k = fetch just a single record with the specified key (hash)
        -s = start of range to query (hash value - defaults to 0x00000000)
        -e = end of range to query   (hash value - defaults to 0xFFFFFFFF)
        -C = query complete hash range
        -c = channel name to query
        -A = query all channels

*******************************************************************************/

module src.mod.dump.DhtDump;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.model.DhtTool;

private import swarm.dht.DhtClient,
               swarm.dht.DhtHash,
               swarm.dht.DhtConst;

private import ocean.core.Array;

private import ocean.text.Arguments;

private import tango.io.Stdout;

private import tango.math.Math : min;

debug private import tango.util.log.Trace;



/*******************************************************************************

    Dht dump tool

*******************************************************************************/

class DhtDump : DhtTool
{
    /***************************************************************************

        Singleton instance of this class, used in static methods.
    
    ***************************************************************************/

    private static typeof(this) singleton;

    static private typeof(this) instance ( )
    {
        if ( !singleton )
        {
            singleton = new typeof(this);
        }

        return singleton;
    }


    /***************************************************************************

        Parses and validates command line arguments.
        
        Params:
            args = arguments object
            arguments = command line args (excluding the file name)
    
        Returns:
            true if the arguments are valid
    
    ***************************************************************************/
    
    static public bool parseArgs ( Arguments args, char[][] arguments )
    {
        return instance().validArgs(args, arguments);
    }
    
    
    /***************************************************************************
    
        Main run method, called by OceanException.run.
        
        Params:
            args = processed arguments
    
        Returns:
            always true
    
    ***************************************************************************/
    
    static public bool run ( Arguments args )
    {
        instance().process(args);
        return true;
    }


    /***************************************************************************

        Maximum number of characters to output to stdout for each record
    
    ***************************************************************************/
    
    private uint text_limit;
    
    
    /***************************************************************************
    
        True = just count records, false = show text of each record
    
    ***************************************************************************/
    
    private bool count_records;
    
    
    /***************************************************************************
    
        True = output hex values, false = output strings
    
    ***************************************************************************/
    
    private bool hex_output;


    /***************************************************************************

        Internal count of records & bytes received
    
    ***************************************************************************/

    private ulong records, bytes;

    
    /***************************************************************************

        Internal count of records & bytes received for the current channel
    
    ***************************************************************************/

    private ulong channel_records, channel_bytes;


    /***************************************************************************

        Adds command line arguments specific to this tool.
        
        Params:
            args = command line arguments object to add to
    
    ***************************************************************************/

    override protected void addArgs_ ( Arguments args )
    {
        args("count").aliased('n').help("count records, do not dump contents");
        args("hex").aliased('x').help("displays records as hexadecimal dump (default is a string dump)");
        args("limit").params(1).defaults("0xffffffff").aliased('l').help("limits the length of text displayed for each record (defaults to no limit)");
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
        if ( args.exists("key") && args.exists("count") )
        {
            Stderr.formatln("You want to count a single record? The result is 1.");
            return false;
        }
        
        return true;
    }


    /***************************************************************************

        Initialises this instance from the specified command line args.
    
        Params:
            args = command line arguments object to read settings from

    ***************************************************************************/

    override protected void init_ ( Arguments args )
    {
        this.count_records = args.getBool("count");
        this.hex_output = args.getBool("hex");
        this.text_limit = args.getInt!(uint)("limit");

        this.records = 0;
        this.bytes = 0;
    }


    /***************************************************************************

        Called when processing has finished. Outputs total count of records
        processed.

        Params:
            dht = dht client

    ***************************************************************************/

    override protected void finished ( DhtClient dht )
    {
        if ( this.count_records )
        {
            Stdout.format("Total of all channels = {} records ({} bytes) in the specified range\n", this.records, this.bytes);
        }
    }


    /***************************************************************************

        Outputs dht records in the specified hash range in the specified
        channel to stdout.
        
        Params:
            dht = dht client to perform query with
            channel = name of channel to dump
            start = start of hash range to query
            end = end of hash range to query
    
    ***************************************************************************/

    protected void processChannel ( DhtClient dht, char[] channel, hash_t start, hash_t end )
    {
        void receiveRecord ( uint id, char[] key, char[] value )
        {
            if ( key.length && value.length )
            {
                this.outputRecord(channel, key, value);
            }
        }

        this.channel_records = 0;
        this.channel_bytes = 0;

        this.displayChannelSize(dht, channel);

        if ( dht.commandSupported(DhtConst.Command.GetRange) )
        {
            dht.getRange(channel, start, end, &receiveRecord).eventLoop();
        }
        else
        {
            dht.getAll(channel,
                    ( uint id, char[] hash_str, char[] value )
                    {
                        if ( hash_str.length )
                        {
                            auto hash = DhtHash.straightToHash(hash_str);
                            if ( hash >= start && hash <= end )
                            {
                                receiveRecord(id, hash_str, value);
                            }
                        }
                    }).eventLoop();
        }

        this.records += this.channel_records;
        this.bytes += this.channel_bytes;
        if ( this.count_records )
        {
            Stdout.format("Channel {} contains {} records ({} bytes) in the specified range\n\n", channel, this.channel_records, this.channel_bytes);
        }
    }


    /***************************************************************************

        Outputs a single dht record with the specified hash in the specified
        channel to stdout.
        
        Params:
            dht = dht client to perform query with
            channel = channel to query
            key = hash of record to dump
    
    ***************************************************************************/

    protected void processRecord ( DhtClient dht, char[] channel, hash_t key)
    {
        dht.get(channel, key,
                ( uint id, char[] value )
                {
                    if ( value.length )
                    {
                        this.outputRecord(channel, key, value);
                    }
                    else
                    {
                        Stdout.formatln("Record doesn't exist");
                    }
                }).eventLoop();
    }


    /***************************************************************************

        Outputs dht records in the specified channel & hash range to stdout.
        
        Params:
            dht = dht client to perform query with
            channel = channel to query
    
    ***************************************************************************/
    
    private void displayChannelSize ( DhtClient dht, char[] channel  )
    {
        dht.getChannelSize(channel, (hash_t id, char[] address, ushort port, char[] channel, 
                                     ulong records, ulong bytes)
                          {
                              Stdout.formatln("{}:{} {} - {} records, {} bytes", address, port, channel, records, bytes);
                          }).eventLoop();
        Stdout.flush();
    }


    /***************************************************************************

        Outputs a record to stdout
        
        Params:
            channel = channel record came from
            key = record key
            value = record value
    
    ***************************************************************************/

    private void outputRecord ( char[] channel, hash_t key, char[] value )
    {
        DhtHash.HexDigest hash_str;
        DhtHash.toString(key, hash_str);
        this.outputRecord(channel, hash_str, value);
    }

    private void outputRecord ( char[] channel, char[] key, char[] value )
    {
        if ( this.count_records )
        {
            this.channel_records++;
            this.channel_bytes += value.length;

            if ( !(this.channel_records % 1000) )
            {
                Stderr.format("{,10}\b\b\b\b\b\b\b\b\b\b", this.channel_records).flush();
            }
        }
        else
        {
            auto limit = min(this.text_limit, value.length);
            char[] limit_text;
            if ( limit < value.length )
            {
                limit_text = "... [truncated]";
            }

            if ( hex_output )
            {
                Stdout.format("{}: {} -> {:x}{} ({} bytes)\n", channel, key, cast(void[])value[0..limit], limit_text, value.length);
            }
            else
            {
                Stdout.format("{}: {} -> '{}{}' ({} bytes)\n", channel, key, value[0..limit], limit_text, value.length);
            }
        }
    }
}

