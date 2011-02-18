/*******************************************************************************

    DHT node import
    
    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved
    
    version:        January 2011: Initial release
    
    authors:        Gavin Norman
    
    Reads records from a file and puts them to a dht.

    Command line parameters:
        -D = dhtnodes.xml file for destination dht
        -f = name of file to read records from
        -h = display help

*******************************************************************************/

module src.main.dhtimport;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.importer.DhtImport;

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
    if ( DhtImport.parseArgs(args, arguments[1..$]) )
    {
        // run app
        OceanException.run(&DhtImport.run, args);
    }
    else
    {
        args.displayHelp(app_name);
    }
}

