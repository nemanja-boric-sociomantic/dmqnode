/*******************************************************************************

    DHT node dump
    
    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        October 2010: Initial release
    
    authors:        Gavin Norman
    
    Reads records from one or more dht nodes and outputs them to stdout.
    
    Command line parameters:
        -h = display help
        -S = dhtnodes.xml source file
        -k = fetch just a single record with the specified key (hash)
        -s = start of range to query (hash value - defaults to 0x00000000)
        -e = end of range to query   (hash value - defaults to 0xFFFFFFFF)
        -C = query complete hash range
        -c = channel name to query
        -A = query all channels
        -n = count records, do not dump contents
        -x = displays records as hexadecimal dump (default is a string dump)
        -l = limits the length of text displayed for each record

*******************************************************************************/

module src.main.dhtdump;



/*******************************************************************************
 
    Imports 
    
*******************************************************************************/

private import src.mod.dump.DhtDump;

private import ocean.util.OceanException;

private import ocean.text.Arguments;



/*******************************************************************************

    Main

    Params:
        arguments = command line arguments
    
*******************************************************************************/

void main ( char[][] arguments )
{
    auto app_name = arguments[0];

    // Define valid arguments
    scope args = new Arguments();
    if ( DhtDump.parseArgs(args, arguments[1..$]) )
    {
        // run app
        OceanException.run(&DhtDump.run, args);
    }
    else
    {
        args.displayHelp(app_name);
    }
}

