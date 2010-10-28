/*******************************************************************************

    DHT node copy 

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        October 2010: Initial release

    authors:        Lars Kirchhoff

    Simple copy tool that copies the content of a source dht node cluster 
    to a new dst node cluster.

    Command line parameters:
        -h --help           = display help
        -S --source         = source node xml configuration file
        -D --destination    = destination node xml configuration file
        -s --start          = start of hash range to copy
        -e --end            = end of hash range to copy
        -r --ranges         = calculate and display ranges of the new nodes

 ******************************************************************************/

module main.dhtcopy;



/*******************************************************************************

    Imports 

 ******************************************************************************/

private import src.mod.copy.DhtCopy;

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
        OceanException.run(&DhtCopy.run, args);
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
    
    Stderr.formatln("Examples:
    copy all channels from source nodes to destination nodes        
    {0} -S etc/srcnodes.xml -D etc/dstnodes.xml
            
    copy a distinct channel from source nodes to destination nodes        
    {0} -S etc/srcnodes.xml -D etc/dstnodes.xml -c profiles

    copy all channels from source nodes to destination nodes and use 
    compression for destination nodes
    {0} -S etc/srcnodes.xml -D etc/dstnodes.xml -z
            
    get hash ranges for a number of  nodes    
    {0} -r -n 8

    get a list of channels in the source nodes 
    {0} -S etc/srcnodes.xml -D etc/dstnodes.xml -l                      
    ", app_name);
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
    args("help")        .aliased('?').aliased('h').help("display this help");
    
    args("source")      .params(1).aliased('S').help("path of dhtnodes.xml file defining nodes to copy from");
    args("destination") .params(1).aliased('D').help("path of dhtnodes.xml file defining nodes to copy to");
    args("start")       .params(1).aliased('s').defaults("0x00000000").help("start of range to query (hash value - defaults to 0x00000000)");
    args("end")         .params(1).aliased('e').defaults("0xFFFFFFFF").help("end of range to query (hash value - defaults to 0xFFFFFFFF)");
    args("range")       .params(1).aliased('r').help("calculate and display ranges for the given number of nodes");
    args("list")        .params(0).aliased('l').help("get a list of channels in the source nodes");
    args("channel")     .params(1).aliased('c').help("name of the channel to copy (optional)");
    args("compression") .params(0).aliased('z').help("enable compression (optional)");
    
    if (!args.parse(arguments))
    {
        return false;
    }
    
    if ( (args.getString("source").length != 0 && args.getString("destination").length != 0)
        ||  args.getInt!(uint)("range") != 0 || args.getBool("list"))        
    { 
        return true;
    }
    
    return false;
}

