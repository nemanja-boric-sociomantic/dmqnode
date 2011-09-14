/*******************************************************************************

    Queue Producer

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        July 2011: Initial release

    authors:        Gavin Norman

    Micro app which consumes from the specified queue channel.

    Command line args:
        -S = path of queue nodes ini file
        -c = name of channel to consume
        -d = (optional) dumps received records to stdout

*******************************************************************************/

module src.main.queueproducer;



/*******************************************************************************

    Imports 

*******************************************************************************/

private import src.mod.producer.QueueProducer;

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
    if ( QueueProducer.parseArgs(args, arguments[1..$]) )
    {
        // run app
        OceanException.run(&QueueProducer.run, args);
    }
    else
    {
        args.displayHelp(app_name);
    }
}

