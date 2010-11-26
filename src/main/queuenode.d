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
private import core.Terminate;

private import core.config.MainConfig;

private import ocean.sys.CmdPath,
               ocean.sys.SignalHandler;

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
    MainConfig.init(args[0]);
    
    queue = new QueueDaemon();
    
    SignalHandler.register(SignalHandler.AppTermination, &terminate);
    
    queue.run();
    
    delete queue;
}

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
    
    scope (exit) Terminate.terminating = true;
    
    return Terminate.terminating;
}
