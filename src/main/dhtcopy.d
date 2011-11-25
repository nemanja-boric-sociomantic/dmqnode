/*******************************************************************************

    DHT node copy 

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        October 2010: Initial release
                    February 2011: Re-written using SourceDhtTool base class

    authors:        Gavin Norman

    Copies data from a source dht to the destination dht.

    Can also be used (with the -X flag) to compare the contents of two dhts.

    Command line parameters:
        -S = dhtnodes.xml source file
        -D = dhtnodes.xml file for destination dht
        -h = display help
        -k = copy just a single record with the specified key (hash)
        -s = start of range to copy (hash value - defaults to 0x00000000)
        -e = end of range to copy   (hash value - defaults to 0xFFFFFFFF)
        -C = copy complete hash range (0x00000000 .. 0xFFFFFFFF)
        -c = channel name to copy
        -A = copy all channels
        -X = compare specified data in source & destination dhts (do not copy)

 ******************************************************************************/

module src.main.dhtcopy;



/*******************************************************************************

    Imports 

*******************************************************************************/

private import src.mod.copy.DhtCopy;

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

    scope args = new Arguments;
    scope app = new DhtCopy;

    if ( app.parseArgs(app_name, args, arguments[1..$]) )
    {
        app.run(args);
    }
}

