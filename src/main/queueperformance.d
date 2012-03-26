/*******************************************************************************

    Queue performance tester

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        October 2011: Initial release

    authors:        Gavin Norman

    Repeatedly performs a series of one or more pushes, followed by an equal
    number of pops. The time taken per request and for the whole group of push /
    pop requests is measured.

    Command line args:
        -S = path of queue nodes ini file
        -c = the number of pushes / pops to perform sequentially before
             switching from pushing to popping or vice versa (default is 1000)
        -p = the number of parallel pushes / pops to perform (default is 1)
        -s = size of record to push / pop (in bytes, default is 1024)

*******************************************************************************/

module src.main.queueperformance;



/*******************************************************************************

    Imports 

*******************************************************************************/

private import src.mod.performance.QueuePerformance;

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

    scope args = new Arguments;
    scope performance = new QueuePerformance;

    if ( performance.parseArgs(args, arguments[1..$]) )
    {
        performance.run(args);
    }
    else
    {
        args.displayHelp();
    }
}

