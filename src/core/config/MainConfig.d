/*******************************************************************************

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        November 2010: Initial release
    
    authors:        Gavin Norman

*******************************************************************************/

module src.core.config.MainConfig;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.sys.CmdPath;

private import ocean.util.Config,
               ocean.util.OceanException,
               ocean.util.TraceLog;

private import tango.util.log.AppendFile;

debug private import tango.util.log.Trace;



/*******************************************************************************

    Main config

*******************************************************************************/

struct MainConfig
{
public static:

    // TODO: add other config parameters (see server.QueueDaemon) into this struct
    
    /***************************************************************************
    
        Trace
    
    ***************************************************************************/

    bool show_channel_trace;

    uint channel_trace_update;


    /***************************************************************************
    
        Reads static member variables from the config file in etc/config.ini
    
    ***************************************************************************/
    
    void init ( char[] command_path )
    {
        CmdPath cmdpath;
        cmdpath.set(command_path);

        Config.init(cmdpath.prepend("etc", "config.ini"));
    
        // Trace
        show_channel_trace = Config.Bool["Trace", "show_channel_trace"];
        channel_trace_update = Config.Int["Trace", "channel_trace_update"];
    }
}

