/*******************************************************************************

    DHT node dump

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        October 2010: Initial release

    authors:        Gavin Norman

    Reads records from one or more dht nodes and outputs them to stdout or to a
    file.

    Command line parameters:
        -n = count records, do not dump contents
        -x = displays records as hexadecimal dump (default is a string dump)
        -l = limits the length of text displayed for each record
        -f = dumps records to a file, instead of to stdout
        -r = dumps raw records exactly as stored in the node, without
             decompressing
        -t = type of dht (memory / logfiles)

    Inherited from super class:
        -h = display help
        -S = dhtnodes.xml source file
        -k = fetch just a single record with the specified key (hash)
        -s = start of range to query (hash value - defaults to 0x00000000)
        -e = end of range to query   (hash value - defaults to 0xFFFFFFFF)
        -C = query complete hash range
        -c = channel name to query
        -A = query all channels

    TODO: add a flag (only valid in combination with -f) to specify the
    directory which files are written into.

*******************************************************************************/

module src.mod.dump.DhtDump;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.model.SourceDhtTool;

private import swarm.dht.DhtClient,
               swarm.dht.DhtHash,
               swarm.dht.DhtConst;

private import ocean.core.Array;

private import ocean.text.Arguments;

private import ocean.io.serialize.SimpleSerializer;

private import ocean.util.log.PeriodicTrace;

private import tango.io.Stdout;

private import tango.io.device.File;

private import tango.math.Math : min;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Dht dump tool

*******************************************************************************/

public class DhtDump : SourceDhtTool
{
    /***************************************************************************

        Progress tracer.
    
    ***************************************************************************/
    
    private PeriodicTracer trace;


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

        True = dump to file, false = dump to stdout

    ***************************************************************************/

    private bool file_dump;


    /***************************************************************************

        True = dump raw records without decompressing, false = dump decompressed
        records
    
    ***************************************************************************/

    private bool raw_dump;


    /***************************************************************************

        File used for dumping to

    ***************************************************************************/

    private File file;


    /***************************************************************************

        Flag to indicate that the dump file is open
    
    ***************************************************************************/

    private bool file_open;


    /***************************************************************************

        Internal count of records & bytes received
    
    ***************************************************************************/

    private ulong records, bytes;

    
    /***************************************************************************

        Internal count of records & bytes received for the current channel
    
    ***************************************************************************/

    private ulong channel_records, channel_bytes;


    /***************************************************************************

        Flag indicating whether the dhts being copied from/to are memory dhts.
        (true = memory, false = logfiles)

    ***************************************************************************/

    private bool memory;


    /***************************************************************************

        Constructor.
    
    ***************************************************************************/

    public this ( )
    {
        this.file = new File;

        this.trace.interval = 100_000;
        this.trace.static_display = true;
    }


    /***************************************************************************

        Adds command line arguments specific to this tool.
        
        Params:
            args = command line arguments object to add to
    
    ***************************************************************************/

    override protected void addArgs__ ( Arguments args )
    {
        args("type").params(1).required.aliased('t').help("type of dht (memory / logfiles");
        args("count").aliased('n').help("count records, do not dump contents");
        args("hex").aliased('x').help("displays records as hexadecimal dump (default is a string dump)");
        args("limit").params(1).defaults("0xffffffff").aliased('l').help("limits the length of text displayed for each record (defaults to no limit)");
        args("file").aliased('f').help("dumps records to a file, named [channel]");
        args("raw").aliased('r').help("dumps raw records exactly as stored in the node, without decompressing");
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

        switch ( type )
        {
            case "memory":
            case "logfiles":
            break;

            default:
                Stderr.formatln("Dht type must be one of: [memory, logfiles]");
                return false;
        }

        if ( args.exists("file") )
        {
            if ( args.exists("hex") )
            {
                Stderr.formatln("Cannot dump hex data to a file.");
                return false;
            }

            if ( args.exists("count") )
            {
                Stderr.formatln("Cannot dump a record count to file.");
                return false;
            }

            if ( args.exists("limit") )
            {
                Stderr.formatln("Cannot limit the values of records dumped to file.");
                return false;
            }
        }
        return true;
    }


    /***************************************************************************

        Initialises this instance from the specified command line args.
    
        Params:
            args = command line arguments object to read settings from

    ***************************************************************************/

    protected void readArgs__ ( Arguments args )
    {
        this.memory = args.getString("type") == "memory";
        this.count_records = args.getBool("count");
        this.hex_output = args.getBool("hex");
        this.text_limit = args.getInt!(uint)("limit");
        this.file_dump = args.getBool("file");
        this.raw_dump = args.getBool("raw");
        
        this.records = 0;
        this.bytes = 0;
    }


    /***************************************************************************

        Called when processing has finished. Outputs total count of records
        processed.

        Params:
            dht = dht client

    ***************************************************************************/

    override protected void finished ( )
    {
        if ( this.count_records )
        {
            Stdout.format("Total of all channels = {} records ({} bytes) in the specified range\n", this.records, this.bytes);
        }

        if ( this.file_open )
        {
            this.file.close();
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

    protected void processChannel ( char[] channel, hash_t start, hash_t end )
    {
        void getDg ( DhtClient.RequestContext context, char[] hash_str, char[] value )
        {
            this.receiveRecord(channel, hash_str, value);
        }

        this.channel_records = 0;
        this.channel_bytes = 0;

        this.displayChannelSize(channel);

        if ( this.memory )
        {
            auto request = super.dht.getAll(channel, &getDg, &super.notifier);
            this.setRaw(request);
            super.dht.assign(request);
        }
        else
        {
            auto request = super.dht.getRange(channel, start, end, &getDg, &super.notifier);
            this.setRaw(request);
            super.dht.assign(request);
        }

        super.epoll.eventLoop;

        this.records += this.channel_records;
        this.bytes += this.channel_bytes;
        if ( this.count_records || this.file_dump )
        {
            Stdout.format("Channel {} contains {} records ({} bytes) in the specified range\n\n",
                channel, this.channel_records, this.channel_bytes);
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

    protected void processRecord ( char[] channel, hash_t key )
    {
        void getDg ( DhtClient.RequestContext context, char[] value )
        {
            if ( value.length )
            {
                this.outputRecord(channel, key, value);
            }
            else
            {
                Stdout.formatln("Record doesn't exist");
            }
        }

        void getRangeDg ( DhtClient.RequestContext context, char[] hash_str, char[] value )
        {
            this.receiveRecord(channel, hash_str, value);
        }

        if ( this.memory )
        {
            auto request = super.dht.get(channel, key, &getDg, &super.notifier);
            this.setRaw(request);
            super.dht.assign(request);
        }
        else
        {
            auto request = super.dht.getRange(channel, key, key, &getRangeDg, &super.notifier);
            this.setRaw(request);
            super.dht.assign(request);
        }

        super.epoll.eventLoop;

        this.records += this.channel_records;
        this.bytes += this.channel_bytes;
    }


    /***************************************************************************

        Sets a dht request to receive raw (not de-compressed) data if the user
        has requested this.

        Template params:
            T = type of request init struct

        Params:
            request = request init struct from dht client

    ***************************************************************************/

    private void setRaw ( T ) ( T request )
    {
        if ( this.raw_dump )
        {
            request.raw;
        }
    }


    /***************************************************************************

        Outputs the size and record count of the specified channel to stdout.

        Params:
            dht = dht client to perform query with
            channel = channel to query

    ***************************************************************************/
    
    private void displayChannelSize ( char[] channel  )
    {
        super.dht.assign(super.dht.getChannelSize(channel,
                ( DhtClient.RequestContext context, char[] address, ushort port, char[] channel, ulong records, ulong bytes )
                {
                    Stdout.formatln("{}:{} {} - {} records, {} bytes", address, port, channel, records, bytes);
                }, &super.notifier));

        super.epoll.eventLoop();

        Stdout.flush();
    }


    /***************************************************************************

        Handles a record received from the node. If the record is within the
        specified range it is output.

        Params:
            channel = channel record was received from
            key = string containing record's key
            value = record's value

    ***************************************************************************/

    private void receiveRecord ( char[] channel, char[] key, char[] value )
    {
        if ( key.length && value.length )
        {
            auto hash = DhtHash.straightToHash(key);
            if ( hash >= super.range.key1 && hash <= super.range.key2 )
            {
                this.outputRecord(channel, key, value);
            }
        }
    }


    /***************************************************************************

        Outputs a single record.
        
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
        this.channel_records++;
        this.channel_bytes += value.length;

        if ( this.count_records || this.file_dump )
        {
            this.trace.format("{,10}", this.channel_records);
        }

        if ( !this.count_records )
        {
            if ( this.file_dump )
            {
                dumpRecordToFile(channel, key, value);
            }
            else
            {
                dumpRecordToStdout(channel, key, value);
            }
        }
    }


    /***************************************************************************

        Outputs a record to a file. The file is opened if it's not already.

        Params:
            channel = channel record came from
            key = record key
            value = record value
    
    ***************************************************************************/

    private void dumpRecordToFile ( char[] channel, char[] key, char[] value )
    {
        if ( this.file.toString != channel )
        {
            if ( this.file_open )
            {
                this.file.close;
            }

            Stdout.formatln("Writing to file {}", channel);
            this.file.open(channel, File.WriteCreate);
            this.file_open = true;
        }

        SimpleSerializer.write(this.file, key);
        SimpleSerializer.write(this.file, value);
    }


    /***************************************************************************

        Outputs a record to stdout.
    
        Params:
            channel = channel record came from
            key = record key
            value = record value
    
    ***************************************************************************/

    private void dumpRecordToStdout ( char[] channel, char[] key, char[] value )
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

