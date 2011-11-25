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

private import src.main.Version;

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


/*******************************************************************************

    Application description

*******************************************************************************/

private const app_description = "queue node server";


/******************************************************************************

    main method

 ******************************************************************************/

int main ( char[][] cl_args )
{
//    GC.disable;

    auto r = Main.processArgs(cl_args, Version, app_description);

    if ( r.exit )
    {
        return r.exit_code;
    }

    MainConfig.init(cl_args[0]);

    queue = new QueueServer();

    SignalHandler.register(SignalHandler.AppTermination, &terminate);

    queue.run();

    return 0;
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
    debug Trace.formatln("\n{}", SignalHandler.getId(code));

    Terminator.terminating = true;

    queue.shutdown();

    return false;
}

