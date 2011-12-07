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

private import src.main.Version;

private import tango.io.Stdout;

private import ocean.text.Arguments;

private import ocean.util.Main;



/*******************************************************************************

    Application description string

*******************************************************************************/

private const char[] app_descr = "Queue monitor. Displays information about the"
        "amount of data in a queue cluster.";



/*******************************************************************************

    Initialises command line arguments parser with options available to this
    application.

    Returns:
        arguments parser instance

*******************************************************************************/

private Arguments initArguments ( )
{
    auto args = new Arguments;

    args("source").aliased('S').required.params(1).help("source folder");
    args("minimal").aliased('m').help("run the monitor in minimal display mode, to save screen space");
    args("periodic").aliased('p').params(1).defaults("0").help("run the monitor periodically every X seconds");

    return args;
}



/*******************************************************************************

    Main

    Params:
        cl_args = array with raw command line arguments

*******************************************************************************/

private int main ( char[][] cl_args )
{
    auto args = initArguments();

    auto r = Main.processArgsConfig(cl_args, args, Version, app_descr);
    if ( r.exit )
    {
        return r.exit_code;
    }

    auto monitor = new QueueMonitor;
    monitor.run(args);

    return 0;
}

