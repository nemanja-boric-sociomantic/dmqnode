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

    Arguments Handler
    
    Checks if command line argument is valid and starts module
    
    Params:
        arguments = array of command line arguments
    
    Returns: 
        false if no or wrong argument is given

******************************************************************************/

bool isArgument ( char[][] arguments )
{
    Arguments args = new Arguments;

    args("daemon").aliased('d');

    args.parse(arguments);

    if ( args.exists("daemon") )
    {
        return OceanException.run(&DhtNodeServer.run);
    }
    
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

void main ( char[][] args )
{
    MainConfig.init(args[0]);
    
    if (!isArgument(args))
    {
        printUsage();
    }
}

