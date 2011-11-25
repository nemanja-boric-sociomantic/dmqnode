/*******************************************************************************

    DHT node copy 

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        October 2010: Initial release
                    February 2011: Re-written using SourceDhtTool base class

    authors:        Gavin Norman & Mathias Baumann

    Copies data from a source dht to the destination dht.

    Can also be used (with the -X flag) to compare the contents of two dhts.

    Command line parameters:
        -S = dhtnodes.xml source file
        -D = dhtnodes.xml file for destination dht
        -h = display help


 ******************************************************************************/

module src.main.dhttest;



/*******************************************************************************

    Imports 

*******************************************************************************/

private import src.mod.test.DhtTest;

private import ocean.text.Arguments,
               ocean.util.log.SimpleLayout;

private import tango.io.Stdout,
               tango.util.log.Log,
               tango.util.log.AppendConsole;

/*******************************************************************************

    configures the allowed arguments and default arguments
    
    Params:
        args = command line arguments parser instance

*******************************************************************************/

void configureArguments ( Arguments args )
{        
    args("source").aliased('S').params(1).help("xml file containing the dht nodes")
            .required.defaults("dhtnodes.xml");
    args("verbose").aliased('v').params(1,1).defaults("info").help("Verbosity output level");
    args("type").aliased('t').params(1)
        .defaults("memory").restrict(["memory", "logfiles"])
        .required.help("Type of the node");
    args("iterations").aliased('i').params(1).defaults("1").help("How often the test should run (0 for infinite)");
    args("help").aliased('h').help("Display help").halt();
}

void setupLogger ( char[] level )
{
    Log.root.clear;
    Log.root.add(new AppendConsole(new SimpleLayout));
    
    if (level.length > 0) switch (level)
    {
        case "Trace":
        case "trace":
        case "Debug":
        case "debug":
            Log.root.level(Level.Trace);
            break;
            
        case "Info":
        case "info":
            Log.root.level(Level.Info);
            break;
            
        case "Warn":
        case "warn":
            Log.root.level(Level.Warn);
            break;
            
        case "Error":
        case "error":
            Log.root.level(Level.Error);
            break;
            
        case "Fatal":
        case "fatal":
            Log.root.level(Level.Info);
            break;
            
        case "None":
        case "none":
        case "Off":
        case "off":
        case "Disabled":
        case "disabled":
            Log.root.level(Level.None);
            break;
        default:
            throw new Exception("Invalid output level");
    }  
}

/*******************************************************************************

    Main
    
    Params:
        arguments = command line arguments

*******************************************************************************/

void main ( char[][] arguments )
{
    auto app_name = arguments[0];

    // Define valid arguments
    scope args = new Arguments();
    
    configureArguments(args);
    
    if ( args.parse(arguments[1..$]) )
    {
        setupLogger(args("verbose").assigned[0]);
        
        // run app
        (new DhtTest(args)).run();
    }
    else
    {
        args.displayErrors();
        args.displayHelp(app_name);
    }
}

