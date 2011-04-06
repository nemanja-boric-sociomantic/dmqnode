/*******************************************************************************

    DHT node repair

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        March 2011: Initial release

    authors:        Gavin Norman

    Scans dht data files for errors, optionally fixes found errors.

    Note: this application varies from the other dht tools in that it is *not*
    designed to operate via a dht client communicating with a dht. It must be
    run on an individual node in a dht, on the server where the node resides,
    and attempts to repair the node's actual on-disk data files.

    Thus, for a logfiles node it can operate while the node is running, but for
    a memory node it can only operate while the node is shut down.

    Command line parameters:
        -h = display help
        -c = channel name to repair
        -s = start of range to process (hash value - defaults to 0x00000000)
        -e = end of range to process (hash value - defaults to 0xFFFFFFFF)
        -r = repairs any problems found during scanning

    TODO: Memory node support.

*******************************************************************************/

module src.main.dhtrepair;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.repair.DhtRepair;

private import src.core.config.MainConfig;

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

    MainConfig.init(app_name);

    // Define valid arguments
    scope args = new Arguments();
    if ( DhtRepair.parseArgs(args, arguments[1..$]) )
    {
        // run app
        OceanException.run(&DhtRepair.run, args);
    }
    else
    {
        args.displayHelp(app_name);
    }
}

