/*******************************************************************************

    DHT node info

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman

    Display information about a dht - the names of the channels, and optionally
    the number of records & bytes per channel.

    Command line parameters:
        -S = dhtnodes.xml file for dht to query
        -n = display the count of records & bytes per channel
        -h = display help

    TODO: add a flag to show the number of open connections (when the new dht
    node / client is online)

*******************************************************************************/

module src.main.dhtinfo;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.info.DhtInfo;

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
    if ( DhtInfo.parseArgs(args, arguments[1..$]) )
    {
        // run app
        OceanException.run(&DhtInfo.run, args);
    }
    else
    {
        args.displayHelp(app_name);
    }
}

