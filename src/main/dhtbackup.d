/*******************************************************************************

    DHT backup 

    copyright:      Copyright (c) 2009 - 2011 sociomantic labs. 
                    All rights reserved

    version:        January 2011: Initial release

    authors:        Lars Kirchhoff

    Simple backup tool that copies the content of a source dht node cluster 
    to a local dump file that can be imported again.

    Command line parameters:
        -h --help           = display help
        -c --channel        = channel that should be backup
        -S --source         = source node xml configuration file                       
        -o --output         = name of the file to which the data should be 
                              dumped to
        -i --input          = name of the file from which the data should be 
                              read

 ******************************************************************************/

module main.dhtbackup;



/*******************************************************************************

    Imports 

 ******************************************************************************/

private import  src.mod.backup.DhtBackup;

private import  ocean.util.OceanException;

private import  ocean.text.Arguments;

private import  tango.io.Stdout;

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
        OceanException.run(&DhtBackup.run, args);
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
    backup all data of a channel to Stdout        
    {0} -S etc/srcnodes.xml -c profiles

    backup all data of a channel to profiles.dhtb file in tmp      
    {0} -S etc/srcnodes.xml -c profiles -o /tmp/profiles.dhtb
                
    backup all data of a all channels (dhtb files will be put by their
    name into the directory given       
    {0} -S etc/srcnodes.xml -a -d /tmp/
                
    imports all data of profiles.dhtb file in tmp to channel  
    {0} -S etc/srcnodes.xml -c profiles -i /tmp/profiles.dhtb
            
    
    get a list of channels in the source nodes 
    {0} -S etc/srcnodes.xml -l                      
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
    args("channel")     .params(1).aliased('c').help("name of the channel to copy (optional)");
    args("output")      .params(1).aliased('o').help("name of the file to which the data should be dumped to");    
    args("input")       .params(1).aliased('i').help("name of the file from which the data should be read");
    args("list")        .params(0).aliased('l').help("get a list of channels in the source nodes");
    args("dump_all")    .params(0).aliased('a').help("dump all channel at once (each channel is put into a single file with the channel name)");
    args("output_dir")  .params(1).aliased('d').help("output directory for dumping all channels at once");
     
    if (!args.parse(arguments))
    {
        return false;
    }
    
    if (args.getString("output").length == 0 && 
        args.getString("input").length == 0 && args.getBool("list") == 0)
    {
        return false;
    }
    
    if (args.getString("source").length != 0 && 
        (args.getString("channel").length != 0 || 
         (args.getBool("output_dir") && args.getString("output_dir").length != 0)))        
    { 
        return true;
    }
    
    if (args.getString("source").length != 0 && args.getBool("list") != 0)
    {
        return true;
    }
        
    return false;
}

