/*******************************************************************************

    DHT Node Monitor Daemon

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        Jun 2010: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.monitor;

private import tango.util.Arguments;

private import tango.io.Stdout;

private import ocean.util.OceanException;

private import mod.monitor.DhtNodeMonitor;



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

    args.parse(arguments);

    return OceanException.run(&DhtNodeMonitor.run);
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
    	dhtnomon

    Description:
    	dht node monitor - displays the number of records in each channel");
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
    if ( !isArgument(args) ) printUsage();
}

