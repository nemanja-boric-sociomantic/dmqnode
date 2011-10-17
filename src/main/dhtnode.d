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

private import src.mod.node.DhtNode;

private import src.mod.node.config.MainConfig;

private import src.mod.node.util.Terminator;

private import ocean.sys.SignalHandler;

private import tango.io.Stdout;

private import ocean.text.Arguments;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Parse command line arguments looking for options

    Params:
        args = arguments parser
        arguments = array with raw command line arguments

    Returns:
        true if arguments parsed ok, false on error

*******************************************************************************/

private bool parseArguments ( Arguments args, char[][] arguments )
{
    args("config").aliased('c').params(1).help("use the configuration file CONFIG instead of the default <bin-dir>/etc/config.ini");
    args("daemonize").aliased('d').help("start daemonized dht node server [DEPRECATED]");
    args("help").aliased('h').help("display help");

    return args.parse(arguments);
}


/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts dht node.

    Param:
        arguments = array with raw command line arguments

*******************************************************************************/

private int main ( char[][] arguments )
{
    auto app_name = arguments[0];

    auto args = new Arguments;
    auto args_ok   = parseArguments(args, arguments);

    if ( !args_ok || args("help").set )
    {
        args.displayErrors();
        args.displayHelp(app_name);

        return args_ok ? 0 : 1;
    }

    char[] config;
    if ( args("config").set )
    {
        config = args("config").assigned[$-1];
    }

    MainConfig.init(app_name, config);

    SignalHandler.register(SignalHandler.AppTermination, &shutdown);

    auto dht = new DhtNodeServer;
    dht.run;

    return 0;
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

