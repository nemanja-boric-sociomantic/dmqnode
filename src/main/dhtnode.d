/*******************************************************************************

    DHT Node Server Daemon

    copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved

    version:        Jun 2009: Initial release

    authors:        Thomas Nicolai & Lars Kirchhoff

*******************************************************************************/

module src.main.dhtnode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import Version = src.main.Version;

private import src.mod.node.DhtNode;

private import src.mod.node.config.MainConfig;

private import src.mod.node.util.Terminator;

private import ocean.sys.SignalHandler;

private import tango.io.Stdout;

private import ocean.text.Arguments;

private import ocean.util.Main;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Initialises command line arguments parser with options available to this
    application.

    Returns:
        arguments parser instance

*******************************************************************************/

private Arguments initArguments ( )
{
    auto args = new Arguments;

    args("config").aliased('c').params(1).help("use the configuration file CONFIG instead of the default <bin-dir>/etc/config.ini");
    args("daemonize").aliased('d').help("start daemonized dht node server [DEPRECATED]");

    return args;
}


/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts dht node.

    Param:
        arguments = array with raw command line arguments

*******************************************************************************/

private int main ( char[][] arguments )
{
    auto args = initArguments();

    auto run = Main.processArgs(arguments, args, Version.revision, "dht node server");
    if ( run )
    {
        char[] config;
        if ( args.exists("config") )
        {
            config = args.getString("config");
        }

        MainConfig.init(arguments[0], config);

        SignalHandler.register(SignalHandler.AppTermination, &shutdown);

        auto dht = new DhtNodeServer;
        dht.run;
    }

    return run ? 0 : 1;
}


/*******************************************************************************

    SIGINT handler. Sets the termination flag.

    Returns:
        false to prevent the default SIGINT signal handler from being called

*******************************************************************************/

private bool shutdown ( int code )
{
    debug Trace.formatln('\n' ~ SignalHandler.getId(code));

    Terminator.terminating = true;

    return false;
}

