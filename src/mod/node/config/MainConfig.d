/*******************************************************************************

    Ad4Max Analytics - General Configuration

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        March 2010: Initial release

    authors:        Lars Kirchhoff

 ******************************************************************************/

module src.mod.node.config.MainConfig;

/*******************************************************************************

        imports

 ******************************************************************************/

private import  swarm.dht.DhtConst;

private import  ocean.sys.CmdPath;

public  import  ocean.util.Config,
                ocean.util.OceanException,
                ocean.util.TraceLog;

private import  tango.util.log.AppendFile;

private import  Date = tango.time.ISO8601,
                tango.time.Time;

/*******************************************************************************

    MainConfig structure

 ******************************************************************************/

struct MainConfig
{
public static:

    /***************************************************************************

        Definitions of configuration section & key strings

     **************************************************************************/
    
    public const
    {
        struct Path
        {
            public static const:
                char[][] Config         = ["etc", "config.ini"];
                char[][] DhtNodeCfg     = ["etc", "dhtnodes.xml"];
        }
        
        struct Sections
        {
            public static const
                Server                  = "Server",
                Options_Hashtable       = "Options_Hashtable",
                Options_Btree           = "Options_Btree",
                Options_Memory          = "Options_Memory",                
                Log                     = "Log";
        }
        
        struct Keys
        {
            public static const
            
                address                 = "address",
                port                    = "port",
                pidfile                 = "pidfile",
                data_dir                = "data_dir",
                storage_engine          = "storage_engine",
                size_limit              = "size_limit",
                minval                  = "minval",
                maxval                  = "maxval",
                
                bnum                    = "bnum",
                apow                    = "apow",
                fpow                    = "apow",
                compression_mode        = "compression_mode",                
                lmemb                   = "lmemb",
                nmemb                   = "nmemb",
                
                Error                   = "error",
                Trace                   = "trace",
                TraceEnable             = "trace_enable";            
        }
    }
    
    /***************************************************************************

        Log
    
    ***************************************************************************/
    
    char[] error_log;
    
    char[] trace_log;
    
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
    
     **************************************************************************/
    
    public void init ( char[] exepath )
    {   
        this.cmdpath.set(exepath);

        Config.init(this.cmdpath.prepend(this.Path.Config));

        // Log
        error_log = Config.Char["Log", "error"];
        OceanException.setOutput(new AppendFile(cmdpath.prepend([error_log])));
        OceanException.console_output = Config.Bool["Log", "console_echo_error"];

        trace_log = Config.Char["Log", "trace"];
        TraceLog.init(cmdpath.prepend([trace_log]));
        TraceLog.console_enabled = Config.Bool["Log", "console_echo_trace"];

        stats_log = cmdpath.prepend(Config.Char["Log", "stats"]);
        stats_log_enabled = Config.Bool["Log", "stats_log_enabled"];
        console_stats_enabled = Config.Bool["Log", "console_stats_enabled"];
    }
}

