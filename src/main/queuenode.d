/******************************************************************************

    Queue Node Server
    
    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        Octover 2010: Initial release
    
    authors:        David Eckardt


 ******************************************************************************/

module main.queuenode;

/******************************************************************************

    Imports 

 ******************************************************************************/

private import server.QueueDaemon;

private import ocean.util.Config;
private import ocean.sys.CmdPath;
private import ocean.sys.SignalHandler;

debug private import tango.util.log.Trace;

/******************************************************************************

    QueueDaemon instance

 ******************************************************************************/

QueueDaemon queue;

/******************************************************************************

    main method

 ******************************************************************************/

void main ( char[][] args )
{
    CmdPath cmdpath;
    
    cmdpath.set(args[0]);
    
    Config.init(cmdpath.prepend("etc", "config.ini"));
    
    queue = new QueueDaemon();
    
    SignalHandler.register(SignalHandler.AppTermination, &terminate);
    
    queue.run();
    
    delete queue;
}

/******************************************************************************

    Kill application flag

 ******************************************************************************/

bool kill = false;

/******************************************************************************

    Termination signal handler callback method; attempts to gracefully shutdown
    on first invocation and kills the application on subsequent invocations.
    
    Params:
        code = signal code
        
    Returns:
        false if called for the first time or true if called before (true
        indicates that the application should be killed)

 ******************************************************************************/

bool terminate ( int code )
{
    debug Trace.formatln("terminating");
    
    queue.shutdown();
    
    scope (exit) kill = true;
    
    return kill;
}
