/*******************************************************************************

    Ad4Max Analytics - General Configuration

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        March 2010: Initial release

    authors:        Lars Kirchhoff

 ******************************************************************************/

module core.config.MainConfig;

/*******************************************************************************

        imports

 ******************************************************************************/

//private import  core.config.DhtNodesConfig;

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
    /***************************************************************************

        Definitions of configuration section & key strings

     **************************************************************************/
    
    public static const
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
                connection_threads      = "connection_threads",
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

        MainExe object holding absolute path to running executable

     **************************************************************************/

    private static CmdPath cmdpath;
        
    /***************************************************************************

        Initializes configuration
        
        Params
            exepath = path to running executable as given by command line
                      argument 0
    
     **************************************************************************/
    
    public static void init ( char[] exepath )
    {   
        char[] trace_log, error_log, monitor_log;
        
        bool trace_enable;
        
        this.cmdpath.set(exepath);
        
        Config.init(this.cmdpath.prepend(this.Path.Config));
        
        error_log    = Config.getChar   (this.Sections.Log, this.Keys.Error);
        trace_enable = !!Config.getInt  (this.Sections.Log, this.Keys.TraceEnable); 
        
        if (trace_enable)
        {
            trace_log = Config.getChar(this.Sections.Log, this.Keys.Trace);
            TraceLog.init(this.cmdpath.prepend([trace_log]));
        }
        else
        {
            TraceLog.disableTrace;
        }
    
        OceanException.setOutput(new AppendFile(this.cmdpath.prepend([error_log])));
    }
}

