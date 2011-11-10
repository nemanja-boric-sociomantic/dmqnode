/******************************************************************************

    Queue Node Server
    
    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        October 2010: Initial release
    
    authors:        David Eckardt


 ******************************************************************************/

module src.main.queuenode;



/******************************************************************************

    Imports 

 ******************************************************************************/

private import Version = src.main.Version;

private import src.mod.server.QueueServer;

private import src.mod.server.util.Terminator;

private import src.mod.server.config.MainConfig;

private import ocean.sys.CmdPath,
               ocean.sys.SignalHandler;

private import ocean.util.Main;

private import tango.core.Memory;

debug private import ocean.util.log.Trace;



/******************************************************************************

    QueueServer instance

 ******************************************************************************/

QueueServer queue;


/******************************************************************************

    main method

 ******************************************************************************/

int main ( char[][] arguments )
{
//    GC.disable;

    auto run = Main.processArgs(arguments, Version.revision, "queue node server");
    if ( run )
    {
        MainConfig.init(arguments[0]);

        queue = new QueueServer();

        SignalHandler.register(SignalHandler.AppTermination, &terminate);

        queue.run();
    }

    return run ? 0 : 1;
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

    return false;
}

