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

*******************************************************************************/

private import  src.mod.node.config.MainConfig;

private import  src.mod.node.servicethreads.ServiceThreads,
                src.mod.node.servicethreads.StatsThread,
                src.mod.node.servicethreads.MaintenanceThread;

private import  ocean.io.select.model.ISelectClient;

private import  swarm.dht.DhtConst;
private import  swarm.dht.DhtNode;
private import  swarm.dht.DhtHash;

private import  swarm.dht.node.storage.MemoryStorageChannels;
private import  swarm.dht.node.storage.LogFilesStorageChannels;

private import  swarm.dht.node.model.IDhtNode;

private import  tango.util.log.Log, tango.util.log.AppendConsole;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    DhtDaemon

******************************************************************************/

class DhtDaemon
{
    /***************************************************************************
    
        alias for Memory node 
    
    **************************************************************************/
    
    private alias DhtNode!(MemoryStorageChannels, char[]) MemoryNode;
    
    /***************************************************************************
    
        alias for LogFiles node 
    
    **************************************************************************/
    
    private alias DhtNode!(LogFilesStorageChannels, char[], size_t) LogFilesNode;
    
    /***************************************************************************
    
        alias for node item 
    
    **************************************************************************/
    
    private alias DhtConst.NodeItem NodeItem;
    
    /***************************************************************************

        Storage engine type enum

    ***************************************************************************/

    private enum Storage
    {
        None,
        HashTable,
        Btree,
        FileSystem,
        Memory,
        LogFiles
    }

    /***************************************************************************
    
        Dht node instance
    
    **************************************************************************/
    
    private IDhtNode node;

    /***************************************************************************
    
        Service threads handler
    
    **************************************************************************/

    private ServiceThreads service_threads;

    /***************************************************************************
    
         Constructor
    
    **************************************************************************/

    public this ( )
    {
        auto min = Config.Char["Server", "minval"];
        auto max = Config.Char["Server", "maxval"];
        auto min_hash = DhtHash.toHashRangeStart(min);
        auto max_hash = DhtHash.toHashRangeEnd(max);

        ulong size_limit = Config.get!(ulong)("Server", "size_limit");
        char[] data_dir = Config.get!(char[])("Server", "data_dir");

        NodeItem node_item = NodeItem(Config.Char["Server", "address"], Config.Int["Server", "port"]);

        Storage storage = this.getStorageConfiguration();
        assertEx(storage != Storage.None, "Invalid storage engine type");

        switch (storage)
        {
            case Storage.Memory :
                auto memory_node = new MemoryNode(node_item, min_hash, max_hash,
                        size_limit, data_dir);
                memory_node.error_callback(&this.dhtError);
                this.node = memory_node;
                break;

            case Storage.LogFiles :
                size_limit = 0;

                auto logfiles_node = new LogFilesNode(node_item, min_hash, max_hash,
                        size_limit, data_dir, this.getLogFilesWriteBuffer());
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

        Callback for exceptions inside the dht node event loop. Writes errors to
        the error.log file, and optionally to the console (if the
        Log/console_echo_errors config parameter is true).

        Params:
            exception = exception which occurred
            event_info = info about epoll event during which exception occurred

    **************************************************************************/

    private void dhtError ( Exception exception, IAdvancedSelectClient.Event event_info )
    {
        OceanException.Warn("Exception caught in eventLoop: {}", exception.msg);
    }

    /***************************************************************************
    
        Get storage configuration
    
    **************************************************************************/
    
    private Storage getStorageConfiguration ( )
    {
        switch (Config.get!(char[])("Server", "storage_engine"))
        {
            case "hashtable" :
                return Storage.HashTable;
        
            case "btree" : 
                return Storage.Btree;
        
            case "filesystem" : 
                return Storage.FileSystem;
        
            case "memory" : 
                return Storage.Memory;
        
            case "logfiles" :
                return Storage.LogFiles;
        
            default :
        }
        
        return Storage.None;
    }
    
    /***************************************************************************
    
        Reads the file output write buffer size for the LogFiles storage engine,
        using the default if not specified in configuration.
        
    **************************************************************************/
    
    private size_t getLogFilesWriteBuffer ()
    {
        size_t wbs = LogFilesStorageChannels.DefaultWriteBufferSize;

        Config.get(wbs, "Options_LogFiles", "write_buffer_size");

        return wbs;
    }
}

