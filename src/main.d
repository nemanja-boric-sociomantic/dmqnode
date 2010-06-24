/*******************************************************************************

    DHT Node Server Daemon

    copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved

    version:        Jun 2009: Initial release

    authors:        Thomas Nicolai & Lars Kirchhoff

*******************************************************************************/

module  main;

private import tango.util.Arguments;

private import tango.io.Stdout;

private import ocean.util.OceanException;

private import mod.server.DhtNodeServer;

/*******************************************************************************

    Arguments Handler

********************************************************************************/

/**
 * Checks if command line argument is valid and starts module
 *
 * Params:
 *     arguments = array of command line arguments
 *
 * Returns: false if no or wrong argument is given
 */
bool isArgument( char[][] arguments )
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

    Usage

********************************************************************************/

/**
 * Prints usage to Stdout
 *
 */
void printUsage()
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

********************************************************************************/

/**
 * Main.
 *
 * Params:
 *     args = command line arguments
 */
void main ( char[][] args )
{
    /*
    int[char[]] x;
    
    x["Apfel"] = 0;
    x["Birne"] = 0;
    x["Citrone"] = 0;
    x["Erdbeere"] = 0;
    
    Stderr.formatln("{}", x);
    
    foreach (key; x.keys)
    {
        Stderr.formatln("\t{:X2}", cast (ubyte[]) key);
        
        x.remove(key);
    }
    
    return;
    */
    
    if ( !isArgument(args) ) printUsage();
}

