/*******************************************************************************

    DHT node dump
    
    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        October 2010: Initial release
    
    authors:        Gavin Norman
    
    Reads records from one or more dht nodes and outputs them to stdout.
    
    Command line parameters:
        -h = display help
        -S = dhtnodes.xml source file
        -s = start of range to query (hash value - defaults to 0x00000000)
        -e = end of range to query   (hash value - defaults to 0xFFFFFFFF)
        -c = channel name to query
        -n = count records, do not dump contents
        -A = query all channels
        -x = displays records as hexadecimal dump (default is a string dump)
        -l = limits the length of text displayed for each record
        -k = fetch just a single record with the specified key (hash)

*******************************************************************************/

module src.main.dump;



/*******************************************************************************
 
    Imports 
    
*******************************************************************************/

private import src.mod.dump.DhtDump;

private import ocean.util.OceanException;

private import ocean.text.Arguments;

private import tango.util.log.Trace;



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
    if ( parseArgs(args, arguments[1..$]) )
    {
        // run app
        OceanException.run(&DhtDump.run, args);
    }
    else
    {
        args.displayHelp(app_name);
    }
}



/*******************************************************************************

    Parses command line arguments and checks them for validity.
    
    Params:
        args = argument parser
        arguments = command line arguments

    Returns:
        true if the command line arguments are valid and the program should be
        executed

*******************************************************************************/

bool parseArgs ( Arguments args, char[][] arguments )
{
    args("help").aliased('?').aliased('h').help("display this help");
    args("source").params(1).required().aliased('S').help("path of dhtnodes.xml file defining nodes to dump");
    args("start").params(1).aliased('s').help("start of range to query (hash value - defaults to 0x00000000)");
    args("end").params(1).aliased('e').help("end of range to query (hash value - defaults to 0xFFFFFFFF)");
    args("channel").conflicts("all_channels").params(1).aliased('c').help("channel name to query");
    args("all_channels").conflicts("channel").aliased('A').help("query all channels");
    args("count").aliased('n').help("count records, do not dump contents");
    args("hex").aliased('x').help("displays records as hexadecimal dump (default is a string dump)");
    args("limit").params(1).defaults("0xffffffff").aliased('l').help("limits the length of text displayed for each record (defaults to no limit)");
    args("key").params(1).aliased('k').help("fetch just a single record with the specified key (hash)");

    // Parse aguments
    if ( !args.parse(arguments) )
    {
        Trace.formatln("Invalid arguments");
        return false;
    }

    // start must be <= end
    if ( args.getInt!(uint)("start") > args.getInt!(uint)("end") )
    {
        Trace.formatln("Range start must be <= end");
        return false;
    }

    // cannot do a single key request and a range request
    if ( args.exists("key") )
    {
        if ( args.exists("start") || args.exists("end") )
        {
            return false;
        }
    }

    return true;
}

