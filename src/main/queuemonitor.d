/*******************************************************************************

    Queue node monitor

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.main.queuemonitor;



/*******************************************************************************

    Imports 

*******************************************************************************/

private import src.mod.monitor.QueueMonitor;

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
    if ( QueueMonitor.parseArgs(args, arguments[1..$]) )
    {
        // run app
        OceanException.run(&QueueMonitor.run, args);
    }
    else
    {
        args.displayHelp(app_name);
    }
}

