/*******************************************************************************

    DHT Node Server Daemon

    copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved

    version:        Jun 2009: Initial release

    authors:        Thomas Nicolai & Lars Kirchhoff

******************************************************************************/

module src.main.dhtnode;

/*******************************************************************************

    Imports

******************************************************************************/

private import  src.mod.node.config.MainConfig;

private import  src.mod.node.DhtNodeServer;

private import  ocean.util.OceanException;

private import  tango.io.Stdout;

private import  ocean.text.Arguments;

/*******************************************************************************

    Parse command line arguments looking for options

    Params:
        arguments = array with raw command line arguments

    Returns:
        Parsed arguments

******************************************************************************/

Arguments parseArguments ( char[][] arguments )
{
    Arguments args = new Arguments;

    args("daemon").aliased('d');
    args("config").aliased('c').params(1);

    args.parse(arguments);

    return args;
}

/*******************************************************************************

    Validate the parsed command line arguments

    Params:
        args = command line arguments

    Returns:
        false if wrong arguments are given

******************************************************************************/

bool validateArguments ( Arguments args )
{
    if ( args.exists("daemon") )
        return true;

    return false;
}

/*******************************************************************************

    Print usage

******************************************************************************/

void printUsage ()
{
    Stdout.formatln("
    Usage:
        dhtnode [-d] [-c CONFIG]

    Description:
        dht node server daemon

    Parameter:
        -d, --daemon         start local dht node server
        -c, --config CONFIG  use the configuration file CONFIG instead of the
                             default <bin-dir>/etc/config.ini.

    Example:
        dhtnode -d -c path/to/config.ini
    ");
}

/*******************************************************************************

    Main (Start)

    Param:
        args = command line arguments

******************************************************************************/

int main ( char[][] args )
{
    auto arguments = parseArguments(args);

    if (!validateArguments(arguments))
    {
        printUsage();
        return 1;
    }

    char[] config;
    if (arguments("config").set)
        config = arguments("config").assigned[$-1];

    MainConfig.init(args[0], config);

    if (OceanException.run(&DhtNodeServer.run))
        return 0;

    return 2;
}

