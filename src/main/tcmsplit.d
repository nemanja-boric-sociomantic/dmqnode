/*******************************************************************************

    Memory node dump file splitter

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        December 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.main.tcmsplit;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.tcmsplit.TcmSplitter;

private import src.main.Version;

private import tango.io.Stdout;

private import ocean.text.Arguments;

private import ocean.util.Main;



/*******************************************************************************

    Application description string

*******************************************************************************/

private const char[] app_descr = "Memory node dump file splitter. Combines and"
        "then splits a set of .tcm files over the specified number of nodes.";



/*******************************************************************************

    Initialises command line arguments parser with options available to this
    application.

    Returns:
        arguments parser instance

*******************************************************************************/

private Arguments initArguments ( )
{
    auto args = new Arguments;

    args("source").aliased('S').required.params(1).help("source folder (multiple "
            "source folders may be specified)");
    args("destination").aliased('D').required.params(1).help("destination folder");
    args("nodes").aliased('n').required.params(1).help("file containing a list of the hash ranges of the destination nodes");

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

    auto splitter = new TcmSplitter;
    splitter.run(args);

    return 0;
}

