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

void main ( char[][] cl_args )
{
    scope args = new Arguments(cl_args[0]);
    scope performance = new DhtPerformance;

    if ( performance.parseArgs(args, cl_args[1..$]) )
    {
        performance.run(args);
    }
    else
    {
        args.displayHelp();
    }
}

