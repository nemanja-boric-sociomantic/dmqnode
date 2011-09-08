/*******************************************************************************

    Queue node tester

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.main.queuetest;



/*******************************************************************************

    Imports 

*******************************************************************************/

private import src.mod.test.QueueTest,
               src.mod.test.SimpleLayout;

private import ocean.text.Arguments;

debug private import ocean.util.log.Trace;

private import tango.io.Stdout,
               tango.util.log.Log,
               tango.util.log.AppendConsole;

/*******************************************************************************

    Displays available command tests

*******************************************************************************/

void displayCommands()
{
    Trace.formatln("Available commands:\n"
            "\tconsumer    - pushes and consumes items\n"
            "\tpopper      - pushes and pops items\n"
            "\tfillConsume - pushes till queue is full, then consumes till queue is empty\n"
            "\tfillPop     - pushes till queue is full, then pops till queue is empty\n");
}

/*******************************************************************************

    Displays available parallel tests

*******************************************************************************/

void displayParallelOptions()
{
    Trace.formatln("Available parallel options:\n"
            "\tsingle - one single test, no parallel execution\n"
            "\tsame   - several tests running in parallel on the same channels\n"
            "\tother  - several tests running in parallel on other channels\n");
}

/*******************************************************************************

    configures the allowed arguments and default arguments
    
    Params:
        args = command line arguments parser instance

*******************************************************************************/

void configureArguments ( Arguments args )
{        
    args("commands").aliased('t').params(0, 3).help("Command tests to run")
            .defaults("popper").defaults("consumer").defaults("fillPop")
            .restrict(["consumer", "popper", "fillPop"]);
    args("config").required().aliased('C').params(1).help("Queue configuration file")
            .defaults("etc/queuenodes.ini");
    args("parallel").aliased('p').params(0, 3).help("Parallel execution options")
            .defaults("other").defaults("single").defaults("same")
            .restrict(["other", "single", "same"]);
    args("verbose").aliased('v').params(1,1).defaults("info").help("Verbosity output level");
    args("amount").aliased('a').params(1).defaults("10000").help("Amount of items to push");
    args("size").aliased('s').params(1).defaults("10").help("Maximum size an item");
    args("channels").aliased('c').params(1).defaults("3").help("Amount of channels to use for pushMulti*");
    
    args("help").aliased('h').help("Display help");
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

    TODO: tango segfaults if you pass invalid arguments. Investigate.

*******************************************************************************/

int main ( char[][] arguments )
{
    auto app_name = arguments[0];

    // Define valid arguments
    scope args = new Arguments();
    
    configureArguments(args);
    
    if ( !args.parse(arguments) )
    {
	    Trace.format("Error: ");
        Trace.formatln (args.errors(&stderr.layout.sprint));
        Trace.formatln("");
        displayCommands();
        displayParallelOptions();
        args.displayHelp(app_name);
        return 1;
    }
    
    if ( args("help").set )
    {
        displayCommands();
        displayParallelOptions();
        args.displayHelp(app_name);
        return 0;
    }
    
    setupLogger(args("verbose").assigned[0]);
    
    QueueTest.run(args);
    
    return 0;
}

