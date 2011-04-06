/*******************************************************************************

    DHT node ranges reporter
    
    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved
    
    version:        April 2011: Initial release
    
    authors:        Gavin Norman

    TODO

    Command line parameters:
        -S = dhtnodes.xml file for dht to query
        -v = verbose output, displays info per channel per node, and per node
            per channel
        -h = display help
        -c = display the number of connections being handled per node
        -a = display the api version of the dht nodes

*******************************************************************************/

module src.main.dhtranges;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.ranges.DhtRanges;

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
    if ( DhtRanges.parseArgs(args, arguments[1..$]) )
    {
        // run app
        OceanException.run(&DhtRanges.run, args);
    }
    else
    {
        args.displayHelp(app_name);
    }
}

