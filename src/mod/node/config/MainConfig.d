/*******************************************************************************

    DhtNode general configuration

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        March 2010: Initial release

    authors:        Lars Kirchhoff

 ******************************************************************************/

module src.mod.node.config.MainConfig;



/*******************************************************************************

        imports

 ******************************************************************************/

private import swarm.dht.DhtConst;

private import ocean.sys.CmdPath;

public  import ocean.util.Config,
               ocean.util.OceanException,
               ocean.util.TraceLog;

private import tango.util.log.AppendFile;



/*******************************************************************************

    MainConfig structure

    All members are static, used simply as a namespace.

 ******************************************************************************/

public struct MainConfig
{
static:

    /***************************************************************************

        Log
    
    ***************************************************************************/

    char[] stats_log;

    bool stats_log_enabled;

    bool console_stats_enabled;


    /***************************************************************************

        MainExe object holding absolute path to running executable

     **************************************************************************/

    private CmdPath cmdpath;


    /***************************************************************************

        Initializes configuration
        
        Params
            exepath = path to running executable as given by command line
                      argument 0
            config_file = config file to read, if not specified uses
                          <exepath>/etc/config.ini.
    
     **************************************************************************/

    public void init ( char[] exepath, char[] config_file = null )
    {
        cmdpath.set(exepath);

        if (config_file)
            Config.initSingleton(config_file);
        else
            Config.initSingleton(cmdpath.prepend(["etc", "config.ini"]));

        // Log
        auto error_log = Config.Char["Log", "error"];
        OceanException.setOutput(new AppendFile(error_log));
        OceanException.console_output = Config.Bool["Log", "console_echo_error"];

        auto trace_log = Config.Char["Log", "trace"];
        TraceLog.init(trace_log);
        TraceLog.console_enabled = Config.Bool["Log", "console_echo_trace"];

        stats_log = Config.Char["Log", "stats"];
        stats_log_enabled = Config.Bool["Log", "stats_log_enabled"];
        console_stats_enabled = Config.Bool["Log", "console_stats_enabled"];
    }
}

