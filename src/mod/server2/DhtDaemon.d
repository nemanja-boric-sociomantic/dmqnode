/*******************************************************************************

    DHT Node Server Daemon
    
    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved
    
    version:        June 2009:    Initial release
                    January 2011: Asynchronous dht node
    
    authors:        David Eckardt, Gavin Norman 
                    Thomas Nicolai, Lars Kirchhoff

******************************************************************************/

module mod.server2.DhtDaemon;



/*******************************************************************************

    Imports

******************************************************************************/

private import  core.config.MainConfig;

private import  mod.server2.servicethreads.ServiceThreads,
                mod.server2.servicethreads.StatsThread,
                mod.server2.servicethreads.MaintenanceThread;

private import  ocean.io.select.model.ISelectClient;

private import  swarm.dht2.DhtNode;
private import  swarm.dht2.DhtHash;

private import  swarm.dht2.storage.Memory;
private import  swarm.dht2.storage.LogFiles;

private import  swarm.dht2.node.model.IDhtNode;

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


    // TODO
    private ServiceThreads service_threads;


    /***************************************************************************
    
         Constructor
    
    **************************************************************************/
    
    public this ( )
    {
        NodeItem node_item = this.getNodeItemConfiguration();

        // TODO: not sure if we need this, the trace log isn't being used for anything
//        this.setLogger();
        
        uint    number_threads  = Config.get!(uint)("Server", "connection_threads");
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
        this.service_threads.add(new MaintenanceThread(this.node, 30)); // TODO: move update time to config file
        this.service_threads.add(new StatsThread(this.node, 5)); // TODO: move update time to config file

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
        
        char[] minval = Config.getChar("Server", "minval");
        char[] maxval = Config.getChar("Server", "maxval");
        
        node_item.Address  = Config.getChar("Server", "address");
        node_item.Port     = Config.getInt("Server", "port");
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
    
        Set Logger
    
    **************************************************************************/

    // TODO: not sure if we need this, the trace log isn't being used for anything
/*    private void setLogger ( )
    {
        auto log = Log.getLogger("dht.persist");
        log.add(new AppendConsole);
        log.level = Level.Trace;
    }
*/
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

