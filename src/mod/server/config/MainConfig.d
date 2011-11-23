/*******************************************************************************

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        November 2010: Initial release
    
    authors:        Gavin Norman

*******************************************************************************/

module src.mod.server.config.MainConfig;



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

    /***************************************************************************

        Server

    ***************************************************************************/

    char[] address;

    ushort port;

    uint size_limit;

    uint channel_size_limit;

    char[] data_dir;


    /***************************************************************************

        Log

    ***************************************************************************/
    
    char[] error_log;
    
    char[] trace_log;
    
    char[] stats_log;
    
    uint stats_log_period;

    bool stats_log_enabled;

    bool console_stats_enabled;


    /***************************************************************************
    
        Reads static member variables from the config file in etc/config.ini
    
    ***************************************************************************/
    
    void init ( char[] exepath )
    {
        CmdPath cmdpath;
        cmdpath.set(exepath);

        Config().initSingleton(cmdpath.prepend("etc", "config.ini"));

        // Server
        address = Config().Char["Server", "address"];
        port = Config().Int["Server", "port"];
        size_limit = Config().Int["Server", "size_limit"];
        channel_size_limit = Config().Int["Server", "channel_size_limit"];
        data_dir = Config().Char["Server", "data_dir"];

        // Log
        error_log = Config().Char["Log", "error"];
        OceanException.setOutput(new AppendFile(cmdpath.prepend([error_log])));
        OceanException.console_output = Config().Bool["Log", "console_echo_error"];

        trace_log = Config().Char["Log", "trace"];
        TraceLog.init(cmdpath.prepend([trace_log]));
        TraceLog.console_enabled = Config().Bool["Log", "console_echo_trace"];

        stats_log = cmdpath.prepend(Config().Char["Log", "stats"]);
        stats_log_period = Config().Int["Log", "stats_log_period"];

        stats_log_enabled = Config().Bool["Log", "stats_log_enabled"];
        console_stats_enabled = Config().Bool["Log", "console_stats_enabled"];
    }
}

