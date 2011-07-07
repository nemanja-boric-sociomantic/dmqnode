/*******************************************************************************

    DHT Node Server Daemon
    
    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved
    
    version:        June 2009:    Initial release
                    January 2011: Asynchronous dht node
    
    authors:        David Eckardt, Gavin Norman 
                    Thomas Nicolai, Lars Kirchhoff

******************************************************************************/

module src.mod.node.DhtDaemon;



/*******************************************************************************

    Imports

******************************************************************************/

private import  src.core.config.MainConfig;

private import  src.mod.node.servicethreads.ServiceThreads,
                src.mod.node.servicethreads.StatsThread,
                src.mod.node.servicethreads.MaintenanceThread;

private import  ocean.io.select.model.ISelectClient;

private import  swarm.dht.DhtNode;
private import  swarm.dht.DhtHash;

private import  swarm.dht.storage.Memory;
private import  swarm.dht.storage.LogFiles;

private import  swarm.dht.node.model.IDhtNode;

private import  tango.util.log.Log, tango.util.log.AppendConsole;

debug private import tango.util.log.Trace;



/*******************************************************************************

    DhtDaemon

******************************************************************************/

class DhtDaemon
{
    /***************************************************************************
    
        alias for Memory node 
    
    **************************************************************************/
    
    alias       DhtNode!(Memory)                                MemoryNode;
    
    /***************************************************************************
    
        alias for LogFiles node 
    
    **************************************************************************/
    
    alias       DhtNode!(LogFiles, size_t)                      LogFilesNode;
    
    /***************************************************************************
    
        alias for node item 
    
    **************************************************************************/
    
    alias       DhtConst.NodeItem       NodeItem;
    
    /***************************************************************************
    
        alias for storage
    
    **************************************************************************/
    
    alias       DhtConst.Storage.BaseType   Storage;

    /***************************************************************************
    
        Dht node instance
    
    **************************************************************************/
    
    private     IDhtNode                node;

    /***************************************************************************
    
        Service threads handler
    
    **************************************************************************/

    private ServiceThreads service_threads;

    /***************************************************************************
    
         Constructor
    
    **************************************************************************/
    
    public this ( )
    {
        NodeItem node_item = this.getNodeItemConfiguration();

        ulong   size_limit      = Config.get!(ulong)("Server", "size_limit");
        char[]  data_dir        = Config.get!(char[])("Server", "data_dir");

        Storage storage         = this.getStorageConfiguration();

        if (storage == DhtConst.Storage.None)
        {
            throw new Exception("Invalid data storage");
        }
        
        switch (storage)
        {
            case DhtConst.Storage.Memory :
                auto memory_node = new MemoryNode(node_item, data_dir, size_limit);
                memory_node.error_callback(&this.dhtError);
                this.node = memory_node;
                break;
        
            case DhtConst.Storage.LogFiles :
                auto logfiles_node = new LogFilesNode(node_item, data_dir, this.getLogFilesWriteBuffer(), size_limit);
                logfiles_node.error_callback(&this.dhtError);
                this.node = logfiles_node;
                break;

            default:
                throw new Exception("Invalid / unsupported data storage");
                break;
        }

        this.service_threads = new ServiceThreads;
        this.service_threads.add(new MaintenanceThread(this.node, Config.Int["ServiceThreads", "maintenance_sleep"]));
        this.service_threads.add(new StatsThread(this.node, Config.Int["ServiceThreads", "stats_sleep"]));

        OceanException.console_output = Config.get!(bool)("Log", "trace_errors");
    }

    /***************************************************************************
    
        Runs the DHT node
    
    **************************************************************************/
    
    public int run ( )
    {
        this.service_threads.start();

        this.node.eventLoop();

        return true;
    }
    
    /***************************************************************************
    
        Shuts down the DHT node
    
    **************************************************************************/
    
    public void shutdown ( )
    {
        return this.node.shutdown();
    }
    
    /***************************************************************************
    
        Reads NodeItem configuration
    
    **************************************************************************/
    
    private NodeItem getNodeItemConfiguration ()
    {
        NodeItem node_item; 
        
        char[] minval = Config.Char["Server", "minval"];
        char[] maxval = Config.Char["Server", "maxval"];
        
        node_item.Address  = Config.Char["Server", "address"];
        node_item.Port     = Config.Int["Server", "port"];
        node_item.MinValue = DhtHash.toHashRangeStart(minval);
        node_item.MaxValue = DhtHash.toHashRangeEnd(maxval);
        
        return node_item;
    }
    
    /***************************************************************************

        Callback for exceptions inside the dht node event loop. Writes errors to
        the error.log file, and optionally to the console (if the
        Log/trace_errors config parameter is true).

        Params:
            exception = exception which occurred
            event_info = info about epoll event during which exception occurred

    **************************************************************************/

    private void dhtError ( Exception exception, IAdvancedSelectClient.EventInfo event_info )
    {
        OceanException.Warn("Exception caught in eventLoop: {}", exception.msg);
    }

    /***************************************************************************
    
        Get storage configuration
    
    **************************************************************************/
    
    private Storage getStorageConfiguration ()
    {
        switch (Config.get!(char[])("Server", "storage_engine"))
        {
            case "hashtable" :
                return DhtConst.Storage.HashTable;
        
            case "btree" : 
                return DhtConst.Storage.Btree;
        
            case "filesystem" : 
                return DhtConst.Storage.FileSystem;
        
            case "memory" : 
                return DhtConst.Storage.Memory;
        
            case "logfiles" :
                return DhtConst.Storage.LogFiles;
        
            default :
        }
        
        return DhtConst.Storage.None;
    }
    
    /***************************************************************************
    
        Reads the file output write buffer size for the LogFiles storage engine,
        using the default if not specified in configuration.
        
    **************************************************************************/
    
    private size_t getLogFilesWriteBuffer ()
    {
        size_t wbs = LogFiles.DefaultWriteBufferSize;
        
        Config.get(wbs, "Options_LogFiles", "write_buffer_size");
        
        return wbs;
    }
}

