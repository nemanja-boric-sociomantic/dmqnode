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

private import ConfigReader = ocean.util.config.ClassFiller;
private import ocean.util.Config;

private import ocean.util.OceanException;
private import ocean.util.TraceLog;

private import tango.util.log.AppendFile;

debug private import tango.util.log.Trace;



/*******************************************************************************

    Server config values

*******************************************************************************/

private class ServerConfig
{
    ConfigReader.Required!(char[]) address;

    ConfigReader.Required!(ushort) port;

    ulong size_limit = 0; // 0 := no size limit

    ulong channel_size_limit = 0; // 0 := no size limit

    char[] data_dir = "data";
}


/*******************************************************************************

    Logging config values

*******************************************************************************/

private class LogConfig
{
    char[] error_log = "log/error.log";
    bool console_echo_error = false;

    char[] trace_log = "log/trace.log";
    bool console_echo_trace = false;

    char[] stats_log = "log/stats.log";
    bool stats_log_enabled = false;
    bool console_stats_enabled = false;

    uint stats_log_period = 300;
}


/*******************************************************************************

    Config file reader

*******************************************************************************/

public class MainConfig
{
    /***************************************************************************

        Instances of each config class to be read.

    ***************************************************************************/

    static public ServerConfig server;
    static public LogConfig log;


    /***************************************************************************

        Path object holding absolute path to running executable

    ***************************************************************************/

    static private CmdPath cmdpath;


    /***************************************************************************

        Initializes configuration

        Params
            exepath = path to running executable as given by command line
                      argument 0
            config_file = config file to read, if not specified uses
                          <exepath>/etc/config.ini.

    ***************************************************************************/

    static public void init ( char[] exepath, char[] config_file = null )
    {
        cmdpath.set(exepath);

        if ( config_file )
        {
            Config.parse(config_file);
        }
        else
        {
            Config.parse(cmdpath.prepend(["etc", "config.ini"]));
        }

        ConfigReader.fill("Server", server);
        ConfigReader.fill("Log", log);

        OceanException.setOutput(new AppendFile(log.error_log));
        OceanException.console_output = log.console_echo_error;

        TraceLog.init(log.trace_log);
        TraceLog.console_enabled = log.console_echo_trace;
    }
}

