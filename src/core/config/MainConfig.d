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
    
        Reads static member variables from the config file in etc/config.ini
    
    ***************************************************************************/
    
    void init ( char[] exepath )
    {
        char[] trace_log, error_log;
        bool trace_enable;

        CmdPath cmdpath;
        cmdpath.set(exepath);

        Config.init(cmdpath.prepend("etc", "config.ini"));

        // Trace
        show_channel_trace = Config.Bool["Trace", "show_channel_trace"];
        channel_trace_update = Config.Int["Trace", "channel_trace_update"];
        trace_rw_positions = Config.Bool["Trace", "trace_rw_positions"];
        trace_byte_size = Config.Bool["Trace", "trace_byte_size"];

        // Log
        error_log = Config.Char["Log", "error"];
        trace_enable = !!Config.Int["Log", "trace_enable"]; 

        if ( trace_enable )
        {
            trace_log = Config.Char["Log", "trace"];
            TraceLog.init(cmdpath.prepend([trace_log]));
        }
        else
        {
            TraceLog.enabled = false;
        }

        OceanException.setOutput(new AppendFile(cmdpath.prepend([error_log])));
    }
}

