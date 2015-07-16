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

module dmqperformance.main;


/*******************************************************************************

    Imports

*******************************************************************************/

private import dmqperformance.QueuePerformance;


/*******************************************************************************

    Main

    Params:
        arguments = command line arguments

*******************************************************************************/

void main ( char[][] cl_args )
{
    auto app = new QueuePerformance;
    return app.main(cl_args);
}
