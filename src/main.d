/*******************************************************************************

    DHT Node Server Daemon

    copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved

    version:        Jun 2009: Initial release

    authors:        Thomas Nicolai & Lars Kirchhoff

 ******************************************************************************/

module  main;

/*******************************************************************************
 
    Imports 
    
 ******************************************************************************/

private import  core.config.MainConfig;

private import  mod.server.DhtNodeServer;
//
private import  ocean.util.OceanException;
//
private import  tango.io.Stdout;

private import  tango.util.Arguments;

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

    args.prefixShort = ["-"];
    args.prefixLong  = ["--"];

    args.define("d").parameters(0).aka("daemon");

    args.parse(arguments);

    if ( args.contains("d") )
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
        dhtnosed [-d]
                
    Description:
        dht node server daemon

    Parameter:
        -d, --daemon         start local dht node server
                
    Example:
        dhtnosed -d
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