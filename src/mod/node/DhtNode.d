/*******************************************************************************

    DHT Node Server Daemon
    
    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved
    
    version:        June 2009:    Initial release
                    January 2011: Asynchronous dht node
    
    authors:        David Eckardt, Gavin Norman 
                    Thomas Nicolai, Lars Kirchhoff

    TODO: this module is extremely similar to the equivalent in the QueueNode
    project. Find a central place to combine them.

*******************************************************************************/

module src.mod.node.DhtNode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.node.config.MainConfig;

private import src.mod.node.util.Terminator;

private import src.mod.node.periodic.Periodics;
private import src.mod.node.periodic.PeriodicMaintenance;
private import src.mod.node.periodic.PeriodicStats;

private import ocean.core.MessageFiber;
private import ocean.io.select.protocol.generic.ErrnoIOException : IOWarning;

private import ocean.util.Config;

private import ocean.io.select.EpollSelectDispatcher;

private import ocean.io.select.model.ISelectClient;

private import ocean.io.select.event.SignalEvent;

private import swarm.dht.DhtConst;
private import swarm.dht.DhtNode;
private import swarm.dht.DhtHash;

private import swarm.dht.node.storage.model.StorageChannels;

private import swarm.dht.node.storage.MemoryStorageChannels;
private import swarm.dht.node.storage.LogFilesStorageChannels;

private import tango.core.Exception : OutOfMemoryException;

private import ocean.util.log.Trace;

private import ocean.util.OceanException;

private import tango.stdc.posix.signal: SIGINT, SIGTERM, SIGQUIT;



/*******************************************************************************

    DhtNode

*******************************************************************************/

public class DhtNodeServer
{
    /***************************************************************************
    
        Epoll selector instance

    ***************************************************************************/

    private EpollSelectDispatcher epoll;


    /***************************************************************************
    
        Dht node instance
    
    ***************************************************************************/

    private DhtNode node;


    /***************************************************************************
    
        SIGINT, TERM and QUIT handler event

    ***************************************************************************/

    private SignalEvent sigint_event;


    /***************************************************************************

        Periodic processes manager

    ***************************************************************************/

    private Periodics periodics;


    /***************************************************************************
    
        Constructor
    
    ***************************************************************************/

    public this ( )
    {
        this.epoll = new EpollSelectDispatcher;

        this.node = new DhtNode(
                DhtConst.NodeItem(MainConfig.server.address(),
                    MainConfig.server.port()),
                this.newStorageChannels(),
                this.min_hash, this.max_hash, this.epoll);

        this.node.error_callback = &this.nodeError;

        this.sigint_event = new SignalEvent(&this.sigintHandler,
            [SIGINT, SIGTERM, SIGQUIT]);

        this.periodics = new Periodics(this.node, this.epoll);
        this.periodics.add(new PeriodicMaintenance(
            MainConfig.server_threads.maintenance_period));
        this.periodics.add(new PeriodicStats(
            MainConfig.log.stats_log_period));
    }


    /***************************************************************************

        Runs the DHT node

    ***************************************************************************/

    public int run ( )
    {
        this.epoll.register(this.sigint_event);

        this.periodics.register();

        this.node.register(this.epoll);

        Trace.formatln("Starting event loop");
        this.epoll.eventLoop();
        Trace.formatln("Event loop exited");

        return true;
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
        auto minval = MainConfig.server.minval();
        return DhtHash.toHashRangeStart(minval);
    }


    /***************************************************************************

        Returns:
            maximum hash value handled by this node, as defined in config file

    ***************************************************************************/

    private hash_t max_hash ( )
    {
        // TODO: remove this hash range padding, always specify full 32-bit
        // hexadecimal numbers
        auto maxval = MainConfig.server.maxval();
        return DhtHash.toHashRangeEnd(maxval);
    }


    /***************************************************************************

        Callback for exceptions inside the node's event loop. Writes errors to
        the error.log file, and optionally to the console (if the
        Log/console_echo_errors config parameter is true).

        Params:
            exception = exception which occurred
            event_info = info about epoll event during which exception occurred

    ***************************************************************************/

    private void nodeError ( Exception exception,
        IAdvancedSelectClient.Event event_info )
    {
        if ( cast(MessageFiber.KilledException)exception ||
             cast(IOWarning)exception )
        {
            // Don't log these exception types, which only occur on the normal
            // disconnection of a client.
        }
        else if ( cast(OutOfMemoryException)exception )
        {
            OceanException.Warn("OutOfMemoryException caught in eventLoop");
        }
        else
        {
            OceanException.Warn("Exception caught in eventLoop: '{}' @ {}:{}",
                    exception.msg, exception.file, exception.line);
        }
    }


    /***************************************************************************

        SIGINT, TERM and QUIT handler.

        Firstly unregisters all periodics. (Any periodics which are about to
        fire in epoll will still fire, but the setting of the 'terminating' flag
        will stop them from doing anything.)

        Secondly calls the node's shutdown method. This unregisters the select
        listener (stopping any more requests from being processed), then shuts
        down the storage channels.

        Finally shuts down epoll. This will result in the run() method, above,
        returning.

        Params:
            siginfo = info struct about signal which fired

    ***************************************************************************/

    private void sigintHandler ( SignalEvent.SignalInfo siginfo )
    {
        // Due to this delegate being called from epoll, we know that none of
        // the periodics are currently active. (The dump periodic may have
        // caused the memory storage channels to fork, however.)
        // Setting the terminating flag to true prevents any periodics which
        // fire from now on from doing anything (see IPeriodics).
        Terminator.terminating = true;

        this.periodics.shutdown();

        this.node.shutdown;

        this.epoll.shutdown;
    }
}

