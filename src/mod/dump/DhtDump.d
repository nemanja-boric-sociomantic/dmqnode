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

    Inherited from super class:
        -h = display help
        -S = dhtnodes.xml source file
        -k = fetch just a single record with the specified key (hash)
        -s = start of range to query (hash value - defaults to 0x00000000)
        -e = end of range to query   (hash value - defaults to 0xFFFFFFFF)
        -C = query complete hash range
        -c = channel name to query
        -A = query all channels

    TODO: add flag to switch off decompression of fetched records (this only
    works with the new dht client) - essential for proper dump to file backups. 

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

private import tango.io.Stdout;

private import tango.io.device.File;

private import tango.math.Math : min;

debug private import tango.util.log.Trace;



/*******************************************************************************

    Dht dump tool

*******************************************************************************/

class DhtDump : SourceDhtTool
{
    /***************************************************************************

        Singleton parseArgs() and run() methods.
    
    ***************************************************************************/

    mixin SingletonMethods;


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

        Constructor.
    
    ***************************************************************************/

    public this ( )
    {
        this.file = new File;
    }


    /***************************************************************************

        Adds command line arguments specific to this tool.
        
        Params:
            args = command line arguments object to add to
    
    ***************************************************************************/

    override protected void addArgs__ ( Arguments args )
    {
        args("count").aliased('n').help("count records, do not dump contents");
        args("hex").aliased('x').help("displays records as hexadecimal dump (default is a string dump)");
        args("limit").params(1).defaults("0xffffffff").aliased('l').help("limits the length of text displayed for each record (defaults to no limit)");
        args("file").aliased('f').help("dumps records to a file, named [channel].dump");
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

    override protected void readArgs_ ( Arguments args )
    {
        this.count_records = args.getBool("count");
        this.hex_output = args.getBool("hex");
        this.text_limit = args.getInt!(uint)("limit");
        this.file_dump = args.getBool("file");

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

    // TODO: add decompression flag

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

        Outputs the size and record count of the specified channel to stdout.

        Params:
            dht = dht client to perform query with
            channel = channel to query

    ***************************************************************************/
    
    private void displayChannelSize ( DhtClient dht, char[] channel  )
    {
        dht.getChannelSize(channel,
                ( hash_t id, char[] address, ushort port, char[] channel, ulong records, ulong bytes )
                {
                    Stdout.formatln("{}:{} {} - {} records, {} bytes", address, port, channel, records, bytes);
                }).eventLoop();

        Stdout.flush();
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
        if ( !this.file_open )
        {
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

