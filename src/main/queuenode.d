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

private import ocean.text.Arguments;

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

    void initConfig ( char[] app_name, char[] config_file )
    {
        MainConfig.init(app_name, config_file);
    }

    auto args = new Arguments;

    auto r = Main.processArgsConfig(cl_args, args, Version, app_description, &initConfig);

    if ( r.exit )
    {
        return r.exit_code;
    }

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

