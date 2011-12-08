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

private import ocean.util.Config;

private import ocean.io.select.model.ISelectClient;

private import swarm.dht.DhtConst;
private import swarm.dht.DhtNode;
private import swarm.dht.DhtHash;

private import swarm.dht.node.storage.model.StorageChannels;

private import swarm.dht.node.storage.MemoryStorageChannels;
private import swarm.dht.node.storage.LogFilesStorageChannels;

private import swarm.dht.node.model.IDhtNode;

private import tango.core.Exception : OutOfMemoryException;

private import tango.core.Thread;

private import tango.util.log.Log, tango.util.log.AppendConsole;

private import ocean.util.log.Trace;

private import ocean.util.OceanException;



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
                DhtConst.NodeItem(MainConfig.server.address, MainConfig.server.port),
                this.newStorageChannels(),
                this.min_hash, this.max_hash);

        this.node.error_callback = &this.nodeError;

        this.service_threads = new ServiceThreads(&this.shutdown);
        this.service_threads.add(new MaintenanceThread(this.node, MainConfig.server_threads.maintenance_period));
        this.service_threads.add(new StatsThread(this.node, MainConfig.log.stats_log_period));
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
        switch ( cast(char[])MainConfig.server.storage_engine )
        {
            case "memory":
                MemoryStorageChannels.Args args;
                args.bnum = Config.get("Options_Memory", "bnum", args.bnum);

                return new MemoryStorageChannels(MainConfig.server.data_dir,
                        MainConfig.server.size_limit, args);

            case "logfiles":
                LogFilesStorageChannels.Args args;
                args.write_buffer_size = Config.get("Options_LogFiles", "write_buffer_size",
                        args.write_buffer_size);

                return new LogFilesStorageChannels(MainConfig.server.data_dir,
                        0, args); // logfiles node ignores size limit setting

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
        // TODO: remove this hash range padding, always specify full 32-bit
        // hexadecimal numbers
        return DhtHash.toHashRangeStart(MainConfig.server.minval);
    }


    /***************************************************************************

        Returns:
            maximum hash value handled by this node, as defined in config file

    ***************************************************************************/

    private hash_t max_hash ( )
    {
        // TODO: remove this hash range padding, always specify full 32-bit
        // hexadecimal numbers
        return DhtHash.toHashRangeEnd(MainConfig.server.maxval);
    }


    /***************************************************************************

        Callback for exceptions inside the node's event loop. Writes errors to
        the error.log file, and optionally to the console (if the
        Log/console_echo_errors config parameter is true).

        Params:
            exception = exception which occurred
            event_info = info about epoll event during which exception occurred

    ***************************************************************************/

    private void nodeError ( Exception exception, IAdvancedSelectClient.Event event_info )
    {
        if ( cast(OutOfMemoryException)exception )
        {
            OceanException.Warn("OutOfMemoryException caught in eventLoop");
        }
        else
        {
            OceanException.Warn("Exception caught in eventLoop: '{}' @ {}:{}",
                    exception.msg, exception.file, exception.line);
        }
    }
}

