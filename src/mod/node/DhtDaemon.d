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
    
    private alias DhtNode!(MemoryStorageChannels) MemoryNode;
    
    /***************************************************************************
    
        alias for LogFiles node 
    
    **************************************************************************/
    
    private alias DhtNode!(LogFilesStorageChannels) LogFilesNode;
    
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

        // TODO: remove this hash range padding, always specify full 32-bit
        // hexadecimal numbers
        auto min_hash = DhtHash.toHashRangeStart(min);
        auto max_hash = DhtHash.toHashRangeEnd(max);

        ulong size_limit = Config.get!(ulong)("Server", "size_limit");
        char[] data_dir = Config.get!(char[])("Server", "data_dir");

        NodeItem node_item = NodeItem(Config.Char["Server", "address"], Config.Int["Server", "port"]);

        Storage storage = this.getStorageConfiguration();
        assertEx(storage != Storage.None, "Invalid storage engine type");

        switch (storage)
        {
            case Storage.Memory:
                MemoryStorageChannels.Args args;
                args.bnum = Config.Int["Options_Memory", "bnum"];

                auto memory_node = new MemoryNode(node_item, min_hash, max_hash,
                        data_dir, size_limit, args);
                memory_node.error_callback(&this.dhtError);
                this.node = memory_node;
                break;

            case Storage.LogFiles:
                LogFilesStorageChannels.Args args;
                args.write_buffer_size = this.getLogFilesWriteBuffer;

                size_limit = 0; // logfiles node ignores size limit setting

                auto logfiles_node = new LogFilesNode(node_item, min_hash, max_hash,
                        data_dir, size_limit, args);
                logfiles_node.error_callback(&this.dhtError);
                this.node = logfiles_node;
                break;

            default:
                throw new Exception("Invalid / unsupported data storage");
                break;
        }

        this.service_threads = new ServiceThreads;
        this.service_threads.add(new MaintenanceThread(this.node, Config.Int["ServiceThreads", "maintenance_period"]));
        this.service_threads.add(new StatsThread(this.node, Config.Int["Log", "stats_log_period"]));
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
            case "memory":
                return Storage.Memory;

            case "logfiles":
                return Storage.LogFiles;

            default:
                return Storage.None;
        }
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

