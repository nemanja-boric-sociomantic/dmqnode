/*******************************************************************************

    DHT perfomance test

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        October 2010: Initial release

    authors:        Lars Kirchhoff

    Simple copy tool to test the performance of a dht node.
    
    Command line parameters:
        -a      = all node test (default)
        -s      = single node test
        -n      = number of node to test        
  
 ******************************************************************************/

module main.dhtperf;



/*******************************************************************************

    Imports 

 ******************************************************************************/

private import src.mod.perf.DhtPerformance;

private import ocean.util.OceanException;

private import ocean.text.Arguments;

private import tango.io.Stdout;

debug private import tango.util.log.Trace;



/*******************************************************************************

    Main

    Params:
        arguments = command line arguments

 ******************************************************************************/

void main ( char[][] arguments )
{
    auto app_name = arguments[0];
    
    scope args = new Arguments();
    
    if (parseArgs(args, arguments[1..$]))
    {
        OceanException.run(&DhtPerformance.run, args);
    }
    else
    {
        printHelp(args, app_name);
    }
}

/*******************************************************************************
 
    Prints the help text 
     
    Params:
        args = argument parser
        app_name = app_name
         
    Returns:
        void
         
 ******************************************************************************/

void printHelp ( Arguments args, char[] app_name )
{    
    args.displayHelp(app_name);
}



/*******************************************************************************

    Parses command line arguments and checks them for validity.

    Params:
        args = argument parser
        arguments = command line arguments

    Returns:
        true if the command line arguments are valid and the program should be
        executed

 ******************************************************************************/

bool parseArgs ( Arguments args, char[][] arguments )
{
    args("help").aliased('?').aliased('h').help("display this help");
    args("iterations").params(1).aliased('i').help("number of iterations");
    args("connections").params(1).aliased('c').help("number of dht connections");
    args("size").params(1).aliased('s').help("size of entry");
    args("evenloop").params(1).aliased('e').help("size of eventloop stack");
    
    if (!args.parse(arguments))     return false;    
    
    return true;    
}