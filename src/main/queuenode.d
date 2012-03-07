/*******************************************************************************

    Queue Node Server

    copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved

    version:        Jun 2009: Initial release

    authors:        Thomas Nicolai, Lars Kirchhoff, Gavin Norman

*******************************************************************************/

module src.main.dhtnode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.main.Version;

private import src.mod.server.QueueServer;

private import src.mod.server.config.MainConfig;

private import src.mod.server.util.Terminator;

private import ocean.sys.SignalHandler;

private import tango.io.Stdout;

private import ocean.text.Arguments;

private import ocean.util.Main;



/*******************************************************************************

    Initialises command line arguments parser with options available to this
    application.

    Returns:
        arguments parser instance

*******************************************************************************/

private Arguments initArguments ( )
{
    auto args = new Arguments;

    return args;
}


/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts queue node.

    Params:
        arguments = array with raw command line arguments

*******************************************************************************/

private int main ( char[][] arguments )
{
    auto args = initArguments();

    void initConfig ( char[] app_name, char[] config_file )
    {
        MainConfig.init(app_name, config_file);
    }

    auto r = Main.processArgsConfig(arguments, args, Version,
            "queue node server", &initConfig);

    if ( r.exit )
    {
        return r.exit_code;
    }

    auto queue = new QueueServer;
    queue.run;

    return 0;
}

