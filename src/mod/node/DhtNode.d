/*******************************************************************************

    DHT Node Server Daemon
    
    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved
    
    version:        June 2009:    Initial release
                    January 2011: Asynchronous dht node
    
    authors:        David Eckardt, Gavin Norman 
                    Thomas Nicolai, Lars Kirchhoff

*******************************************************************************/

module src.mod.node.DhtNode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.node.config.MainConfig;

private import src.mod.node.servicethreads.ServiceThreads,
               src.mod.node.servicethreads.StatsThread,
               src.mod.node.servicethreads.MaintenanceThread;

private import ocean.io.select.model.ISelectClient;

private import swarm.dht.DhtConst;
private import swarm.dht.DhtNode;
private import swarm.dht.DhtHash;

private import swarm.dht.node.storage.model.StorageChannels;

private import swarm.dht.node.storage.MemoryStorageChannels;
private import swarm.dht.node.storage.LogFilesStorageChannels;

private import swarm.dht.node.model.IDhtNode;

private import tango.core.Thread;

private import tango.util.log.Log, tango.util.log.AppendConsole;

private import ocean.util.log.Trace;



/*******************************************************************************

    DhtNode

*******************************************************************************/

public class DhtNodeServer
{
    /***************************************************************************
    
        Dht node instance
    
    ***************************************************************************/

    private DhtNode node;


    /***************************************************************************
    
        Service threads handler
    
    ***************************************************************************/

    private ServiceThreads service_threads;


    /***************************************************************************
    
        Constructor
    
    ***************************************************************************/

    public this ( )
    {
        this.node = new DhtNode(
                DhtConst.NodeItem(MainConfig.address, MainConfig.port),
                this.newStorageChannels(),
                this.min_hash, this.max_hash);

        this.node.error_callback = &this.dhtError;

        uint stats_log_period = 300;
        Config().get(stats_log_period, "Log", "stats_log_period");

        uint maintenance_period = 3600;
        Config().get(maintenance_period, "ServiceThreads", "maintenance_period");

        this.service_threads = new ServiceThreads(&this.shutdown);
        this.service_threads.add(new MaintenanceThread(this.node, maintenance_period));
        this.service_threads.add(new StatsThread(this.node, stats_log_period));
    }


    /***************************************************************************

        Runs the DHT node

    ***************************************************************************/

    public int run ( )
    {
        this.service_threads.start();

        this.node.eventLoop();

        return true;
    }


    /***************************************************************************

        Service threads finished callback (called when all service threads have
        finished). Shuts down the DHT node.

    ***************************************************************************/

    public void shutdown ( )
    {
        this.node.shutdown();
    }


    /***************************************************************************

        Creates a new instance of the storage channels type specified in the
        config file.

        Returns:
            StorageChannels instance

        Throws:
            if no valid storage channels type is specified in config file

    ***************************************************************************/

    private StorageChannels newStorageChannels ( )
    {
        ulong size_limit = Config().get!(ulong)("Server", "size_limit");
        char[] data_dir = Config().get!(char[])("Server", "data_dir");

        switch ( Config().get!(char[])("Server", "storage_engine") )
        {
            case "memory":
                MemoryStorageChannels.Args args;
                Config().get(args.bnum, "Options_Memory", "bnum");

                return new MemoryStorageChannels(data_dir, size_limit, args);

            case "logfiles":
                LogFilesStorageChannels.Args args;
                Config().get(args.write_buffer_size, "Options_LogFiles", "write_buffer_size");

                size_limit = 0; // logfiles node ignores size limit setting

                return new LogFilesStorageChannels(data_dir, size_limit, args);

            default:
                throw new Exception("Invalid / unsupported data storage");
        }
    }


    /***************************************************************************

        Returns:
            minimum hash value handled by this node, as defined in config file

    ***************************************************************************/

    private hash_t min_hash ( )
    {
        auto min = Config().Char["Server", "minval"];
        
        // TODO: remove this hash range padding, always specify full 32-bit
        // hexadecimal numbers
        return DhtHash.toHashRangeStart(min);
    }


    /***************************************************************************

        Returns:
            maximum hash value handled by this node, as defined in config file

    ***************************************************************************/

    private hash_t max_hash ( )
    {
        auto max = Config().Char["Server", "maxval"];

        // TODO: remove this hash range padding, always specify full 32-bit
        // hexadecimal numbers
        return DhtHash.toHashRangeEnd(max);
    }


    /***************************************************************************

        Callback for exceptions inside the dht node event loop. Writes errors to
        the error.log file, and optionally to the console (if the
        Log/console_echo_errors config parameter is true).

        Params:
            exception = exception which occurred
            event_info = info about epoll event during which exception occurred

    ***************************************************************************/

    private void dhtError ( Exception exception, IAdvancedSelectClient.Event event_info )
    {
        OceanException.Warn("Exception caught in eventLoop: '{}' @ {}:{}",
                exception.msg, exception.file, exception.line);
    }
}

