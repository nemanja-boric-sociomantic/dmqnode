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
        dhtnode [-d]
                
    Description:
        dht node server daemon
    
    Parameter:
        -d, --daemon         start local dht node server
                
    Example:
        dhtnode -d
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

    MainConfig.init(args[0]);

    if (OceanException.run(&DhtNodeServer.run))
        return 0;

    return 2;
}

