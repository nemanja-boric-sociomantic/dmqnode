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

private import src.mod.test.QueueTest;

private import ocean.text.Arguments;

debug private import ocean.util.log.Trace;

private import tango.io.Stdout;

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
    args("commands").aliased('t').params(0, 2).help("Command tests to run")
            .defaults("popper").defaults("consumer").defaults("fillPop")
            .restrict("consumer", "popper", "fillPop");
    args("config").required().aliased('C').params(1).help("Queue configuration file")
            .defaults("etc/queuenodes.ini");
    args("parallel").aliased('p').params(0, 4).help("Parallel execution options")
            .defaults("single").defaults("same").defaults("other")
            .restrict("single", "same", "other");
    args("help").aliased('h').help("Display help");
}

/*******************************************************************************

    Main
    
    Params:
        arguments = command line arguments

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
        stderr (args.errors(&stderr.layout.sprint));
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
    
    QueueTest.run(args);
    
    return 0;
}

