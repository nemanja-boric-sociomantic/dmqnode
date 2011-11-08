/*******************************************************************************

    DHT node info

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman

    Display information about a dht -- the default (with no command line args)
    is a nicely formatted monitor display.

    Display of number of active connections, node api versions, hash ranges, etc
    are possible with command line arguments.

    Command line parameters:
        -S = dhtnodes.xml file for dht to query
        -d = display the quantity of data stored in each node and each channel
        -v = verbose output, displays info per channel per node, and per node
            per channel
        -c = display the number of connections being handled per node
        -a = display the api version of the dht nodes
        -r = display the hash ranges of the dht nodes
        -w = width of monitor display (number of columns)
        -m = show records and bytes as metric (K, M, G, T) in the monitor display

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
    
    // parse arguments
    scope args = new Arguments;
    scope app = new DhtInfo;

    if ( app.parseArgs(arguments[0], args, arguments[1..$]) )
    {
        app.run(args);
    }
}

