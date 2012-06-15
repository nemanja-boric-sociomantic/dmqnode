/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        04/06/2012: Initial release

    authors:        Gavin Norman

    Dht node base class - contains members which are shared by both types of
    node.

*******************************************************************************/

module src.mod.node.model.IDhtNode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.node.config.ServerConfig;
private import src.mod.node.config.StatsConfig;

private import src.mod.node.util.Terminator;

private import src.mod.node.periodic.Periodics;

private import ocean.core.MessageFiber;
private import ocean.io.select.protocol.generic.ErrnoIOException : IOWarning;

private import ocean.util.config.ConfigParser;
private import ConfigReader = ocean.util.config.ClassFiller;

private import ocean.io.select.EpollSelectDispatcher;

private import ocean.io.select.model.ISelectClient;

private import ocean.io.select.event.SignalEvent;

private import ocean.io.Stdout;

private import swarm.dht.DhtConst;
private import swarm.dht.DhtNode;
private import swarm.dht.DhtHash;

private import swarm.dht.node.storage.model.DhtStorageChannels;

private import tango.core.Exception : IOException, OutOfMemoryException;

private import tango.stdc.posix.signal: SIGINT, SIGTERM, SIGQUIT;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("src.mod.node.model.IDhtNode");
}



/*******************************************************************************

    Dht node base class

*******************************************************************************/

abstract public class IDhtNode
{
    /***************************************************************************

        Convenience aliases for derived classes

    ***************************************************************************/

    protected alias .ConfigParser ConfigParser;
    protected alias .ServerConfig ServerConfig;
    protected alias .DhtStorageChannels DhtStorageChannels;


    /***************************************************************************

        Config classes for server and stats

    ***************************************************************************/

    protected ServerConfig server_config;

    protected StatsConfig stats_config;


    /***************************************************************************
    
        Epoll selector instance

    ***************************************************************************/

    private EpollSelectDispatcher epoll;


    /***************************************************************************
    
        Dht node instance
    
    ***************************************************************************/

    private DhtNode node;


    /***************************************************************************
    
        SIGINT, SIGTERM and SIGQUIT handler event

    ***************************************************************************/

    private SignalEvent sigint_event;


    /***************************************************************************

        Periodic processes manager

    ***************************************************************************/

    protected Periodics periodics;


    /***************************************************************************

        Storage channels owned by the node (created by sub-class).

    ***************************************************************************/

    protected DhtStorageChannels storage_channels;


    /***************************************************************************

        Constructor

    ***************************************************************************/

    public this ( ServerConfig server_config, ConfigParser config )
    {
        this.server_config = server_config;

        this.epoll = new EpollSelectDispatcher;

        ConfigReader.fill("Stats", this.stats_config, config);

        this.sigint_event = new SignalEvent(&this.sigintHandler,
            [SIGINT, SIGTERM, SIGQUIT]);

        this.node = new DhtNode(this.node_item, this.newStorageChannels(),
            this.min_hash, this.max_hash, this.epoll);

        this.node.error_callback = &this.nodeError;

        this.periodics = new Periodics(this.node, this.epoll);
    }


    /***************************************************************************

        Do the actual application work. Called by the super class.

        Params:
            args = command line arguments
            config = parser instance with the parsed configuration

        Returns:
            status code to return to the OS

    ***************************************************************************/

    public void run ( )
    {
        this.epoll.register(this.sigint_event);

        this.periodics.register();

        this.node.register(this.epoll);

        log.info("Starting event loop");
        this.epoll.eventLoop();
        log.info("Event loop exited");
    }


    /***************************************************************************

        Creates a new instance of the storage channels. Calls the abstract
        newStorageChannels_() method, which does the actual construction of the
        storage channels instance.

        Returns:
            StorageChannels instance

    ***************************************************************************/

    final protected DhtStorageChannels newStorageChannels ( )
    {
        this.storage_channels = this.newStorageChannels_();
        return this.storage_channels;
    }

    abstract protected DhtStorageChannels newStorageChannels_ ( );


    /***************************************************************************

        Returns:
            node item (address/port) for this node

    ***************************************************************************/

    private DhtConst.NodeItem node_item ( )
    {
        return DhtConst.NodeItem(
            this.server_config.address(), this.server_config.port());
    }


    /***************************************************************************

        Returns:
            minimum hash value handled by this node, as defined in config file

    ***************************************************************************/

    private hash_t min_hash ( )
    {
        // TODO: remove this hash range padding, always specify full 32-bit
        // hexadecimal numbers
        auto minval = this.server_config.minval();
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
        auto maxval = this.server_config.maxval();
        return DhtHash.toHashRangeEnd(maxval);
    }


    /***************************************************************************

        Callback for exceptions inside the node's event loop. Writes errors to
        the log file, and optionally to the console (if the
        Log/console_echo_errors config parameter is true).

        Params:
            exception = exception which occurred
            event_info = info about epoll event during which exception occurred

    ***************************************************************************/

    private void nodeError ( Exception exception,
        IAdvancedSelectClient.Event event_info )
    {
        if ( cast(MessageFiber.KilledException)exception ||
             cast(IOWarning)exception ||
             cast(IOException)exception )
        {
            // Don't log these exception types, which only occur on the normal
            // disconnection of a client.
        }
        else if ( cast(OutOfMemoryException)exception )
        {
            log.error("OutOfMemoryException caught in eventLoop");
        }
        else
        {
            log.error("Exception caught in eventLoop: '{}' @ {}:{}",
                    exception.msg, exception.file, exception.line);
        }
    }


    /***************************************************************************

        SIGINT, TERM and QUIT handler.

        Firstly unregisters all periodics. (Any periodics which are about to
        fire in epoll will still fire, but the setting of the 'terminating' flag
        will stop them from doing anything.)

        Secondly stops the node's select listener (stopping any more requests
        from being processed) and cancels any active requests.

        Thirdly calls the protected shutdown() method, where derived classes can
        add any special shutdown behaviour required.

        Fourthly calls the node's shutdown() method, shutting down the storage
        channels.

        Finally shuts down epoll. This will result in the run() method, above,
        returning.

        Params:
            siginfo = info struct about signal which fired

    ***************************************************************************/

    private void sigintHandler ( SignalEvent.SignalInfo siginfo )
    {
        Stdout.formatln("\nShutting down.");

        // Due to this delegate being called from epoll, we know that none of
        // the periodics are currently active.
        // Setting the terminating flag to true prevents any periodics which
        // fire from now on from doing anything (see IPeriodics).
        Terminator.terminating = true;
        log.info("SIGINT handler");

        log.trace("SIGINT handler: shutting down periodics");
        this.periodics.shutdown();
        log.trace("SIGINT handler: shutting down periodics finished");

        log.trace("SIGINT handler: stopping node listener");
        this.node.stopListener();
        log.trace("SIGINT handler: stopping node listener finished");

        this.shutdown();

        log.trace("SIGINT handler: shutting down node");
        this.node.shutdown();
        log.trace("SIGINT handler: shutting down node finished");

        log.trace("SIGINT handler: shutting down epoll");
        this.epoll.shutdown();
        log.trace("SIGINT handler: shutting down epoll finished");

        log.trace("Finished SIGINT handler");
    }


    /***************************************************************************

        Node shutdown hook. Base class implementation does nothing, but derived
        classes may override to add their own special behaviour here.

    ***************************************************************************/

    protected void shutdown ( ) { }
}

