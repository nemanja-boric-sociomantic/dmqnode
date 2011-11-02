/*******************************************************************************

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        2011: Initial release

    authors:        Gavin Norman

    TODO

*******************************************************************************/

module src.main.dhtperformance;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.performance.DhtPerformance;

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
    scope performance = new DhtPerformance;

    if ( performance.parseArgs(args, arguments[1..$]) )
    {
        performance.run(args);
    }
    else
    {
        args.displayHelp(app_name);
    }
}

