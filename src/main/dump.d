/*******************************************************************************

    DHT Node Dump tool

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        October 2010: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.main.dump;



/*******************************************************************************
 
    Imports 
    
*******************************************************************************/

private import src.mod.dump.DhtDump;

private import ocean.util.OceanException;

private import ocean.text.Arguments;

debug private import tango.util.log.Trace;



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
    args("start").params(1).defaults("0x00000000").aliased('s').help("start of range to query (hash value - defaults to 0x00000000)");
    args("end").params(1).defaults("0xffffffff").aliased('e').help("end of range to query (hash value - defaults to 0xFFFFFFFF)");
    args("channel").conflicts("all_channels").params(1).aliased('c').help("channel name to query");
    args("all_channels").conflicts("channel").aliased('A').help("query all channels");

    // Parse aguments
    if ( !args.parse(arguments) )
    {
        return false;
    }

    if ( args.getInt!(uint)("start") > args.getInt!(uint)("end") )
    {
        return false;
    }

    if ( (args.getString("channel").length && args.getBool("all_channels")) ||
         !(args.getString("channel").length || args.getBool("all_channels")) )
    {
        return false;
    }

    return true;
}

