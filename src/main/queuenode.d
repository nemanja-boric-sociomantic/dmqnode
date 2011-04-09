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

    queue.shutdown();

    return true;
}

