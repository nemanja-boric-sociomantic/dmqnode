/*******************************************************************************

    DHT Node Monitor Daemon

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        Jun 2010: Initial release

    authors:        Gavin Norman

 ******************************************************************************/

module src.monitor;



/*******************************************************************************
 
    Imports
    
 ******************************************************************************/

private import  tango.util.Arguments;

private import  tango.io.Stdout;

private import  ocean.util.OceanException;

private import  mod.monitor.DhtNodeMonitor;



/*******************************************************************************

    Arguments Handler
   
    Checks if command line argument is valid and starts module
 
    Params:
        arguments = array of command line arguments

    Returns: false if no or wrong argument is given

 ******************************************************************************/

bool isArgument( char[][] arguments )
{
    Arguments args = new Arguments;

    args.prefixShort = ["-"];
    args.prefixLong  = ["--"];

    args.define("d").parameters(0).aka("daemon");
    args.define("h").parameters(0).aka("help");
    
    args.parse(arguments);

    if (args.contains("h"))
    {
        return false;
    }
    else
    {
        return OceanException.run(&DhtNodeMonitor.run, args);
    }
}



/*******************************************************************************

    Usage

 ******************************************************************************/

void printUsage ()
{
    Stdout.formatln("
    Usage:
    	./dhtnomon [-d | -h] 
    
            -d      runs monitor in daemon mode (updates display every 60seconds)        
            -h      prints this help
        
    Description:
        DHT node monitor    
                
    	Displays runtime information about the node including the 
        number of records and bytes in each channel.
    ");
}



/*******************************************************************************

    Main (Start)
    
    Params:
        args = command line arguments

 ******************************************************************************/

void main ( char[][] args )
{
    if (!isArgument(args)) 
    {
        printUsage();
    }
}

