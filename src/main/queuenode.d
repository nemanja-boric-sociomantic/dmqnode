/******************************************************************************

    Queue Node Server
    
    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        Octover 2010: Initial release
    
    authors:        David Eckardt


 ******************************************************************************/

module src.main.queuenode;

/******************************************************************************

    Imports 

 ******************************************************************************/

private import src.mod.server.QueueDaemon;

private import src.mod.server.util.Terminator;

private import src.core.config.MainConfig;

private import ocean.sys.CmdPath,
               ocean.sys.SignalHandler;

debug private import tango.util.log.Trace;

/******************************************************************************

    QueueDaemon instance

 ******************************************************************************/

QueueDaemon queue;


private import tango.core.Memory;

/******************************************************************************

    main method

 ******************************************************************************/

void main ( char[][] args )
{
//    GC.disable;
    
    MainConfig.init(args[0]);

    queue = new QueueDaemon();

    SignalHandler.register(SignalHandler.AppTermination, &terminate);

    queue.run();
}

/******************************************************************************

    Termination signal handler callback method; shuts down the queue node.

    Params:
        code = signal code

    Returns:
        true, indicating that the application should be killed

 ******************************************************************************/

bool terminate ( int code )
{
    debug Trace.formatln('\n' ~ SignalHandler.getId(code));

    Terminator.terminating = true;

    queue.shutdown();

    return true;
}

