/*******************************************************************************

    DHT node dump
    
    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        October 2010: Initial release
    
    authors:        Gavin Norman
    
    Reads records from one or more dht nodes and outputs them to stdout.
    
    Command line parameters:
    Inherited from super class:
        -h = display help
        -S = dhtnodes.xml source file
        -k = remove just a single record with the specified key (hash)
        -s = start of range to remove (hash value - defaults to 0x00000000)
        -e = end of range to remove (hash value - defaults to 0xFFFFFFFF)
        -C = remove complete hash range for specified channel(s)
        -c = channel name to remove records from
        -A = remove from all channels

*******************************************************************************/

module src.mod.dump.DhtRemove;



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

    Dht remove tool

*******************************************************************************/

class DhtRemove : DhtTool
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

        Checks whether the parsed command line args are valid.
    
        Params:
            args = command line arguments object to validate
    
        Returns:
            true if args are valid
    
    ***************************************************************************/
    
    override protected bool validArgs_ ( Arguments args )
    {
        if ( args.exists("start") || args.exists("end") )
        {
            Stderr.formatln("Dht nodes do not currently support remove range");
            return false;
        }
        
        return true;
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
        if ( start == hash_t.min && end == hash_t.max )
        {
            dht.removeChannel(channel).eventLoop();
        }
        else
        {
            assert(false, "Dht nodes do not currently support remove range");
        }

        Stdout.formatln("Removed records 0x{:x8} .. 0x{:x8} from {}", start, end, channel);
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
        dht.remove(channel, key).eventLoop();
        Stdout.formatln("Removed record 0x{:x8} from {}", key, channel);
    }
}

