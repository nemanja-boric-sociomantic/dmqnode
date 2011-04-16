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
    
    bool trace_rw_positions;

    bool trace_byte_size;

    uint channel_trace_update;


    /***************************************************************************
    
        ServiceThreads

    ***************************************************************************/

    char[] stats_log;

    bool stats_enabled;

    bool stats_console_enabled;


    /***************************************************************************
    
        Reads static member variables from the config file in etc/config.ini
    
    ***************************************************************************/
    
    void init ( char[] exepath )
    {
        CmdPath cmdpath;
        cmdpath.set(exepath);

        Config.init(cmdpath.prepend("etc", "config.ini"));

        // Trace
        show_channel_trace = Config.Bool["Trace", "show_channel_trace"];
        channel_trace_update = Config.Int["Trace", "channel_trace_update"];
        trace_rw_positions = Config.Bool["Trace", "trace_rw_positions"];
        trace_byte_size = Config.Bool["Trace", "trace_byte_size"];

        // ServiceThreads
        stats_log = Config.Char["ServiceThreads", "stats_log"];
        stats_enabled = Config.Bool["ServiceThreads", "stats_enabled"];
        stats_console_enabled = Config.Bool["ServiceThreads", "stats_console_enabled"];

        // Log
        TraceLog.init(cmdpath.prepend([Config.Char["Log", "trace"]]));
        TraceLog.enabled = Config.Bool["Log", "trace_enable"];
        TraceLog.console_enabled = Config.Bool["Log", "console_trace_enable"];

        auto error_log = Config.Char["Log", "error"];
        OceanException.setOutput(new AppendFile(cmdpath.prepend([error_log])));
    }
}

