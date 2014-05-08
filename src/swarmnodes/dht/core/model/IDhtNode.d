/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        04/06/2012: Initial release
                    30/05/2013: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

    Dht node base class - contains members which are shared by both types of
    node.

*******************************************************************************/

module swarmnodes.dht.core.model.IDhtNode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.dht.core.config.ServerConfig;
private import swarmnodes.common.config.PerformanceConfig;
private import swarmnodes.common.config.StatsConfig;

private import swarmnodes.common.util.Terminator;

private import swarmnodes.common.periodic.Periodics;
private import swarmnodes.common.periodic.PeriodicWriterFlush;

private import swarmnodes.dht.DhtNode;
private import swarmnodes.dht.storage.model.DhtStorageChannels;

private import ocean.core.MessageFiber;
private import ocean.io.select.protocol.generic.ErrnoIOException : IOWarning;

private import ocean.util.config.ConfigParser;
private import ConfigReader = ocean.util.config.ClassFiller;

private import ocean.io.select.EpollSelectDispatcher;

private import ocean.io.select.client.model.ISelectClient;

private import ocean.io.select.client.SignalEvent;

private import ocean.io.Stdout;

private import Hash = ocean.text.convert.Hash;

private import swarm.dht.DhtConst;
private import swarm.dht.DhtHash;

private import tango.core.Exception : IOException, OutOfMemoryException;

private import tango.stdc.posix.signal: SIGINT, SIGTERM, SIGQUIT;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("swarmnodes.node.model.IDhtNode");
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

        Config classes for server, performance and stats

    ***************************************************************************/

    protected ServerConfig server_config;

    protected PerformanceConfig performance_config;

    protected StatsConfig stats_config;


    /***************************************************************************

        Epoll selector instance

    ***************************************************************************/

    protected const EpollSelectDispatcher epoll;


    /***************************************************************************

        Dht node instance

    ***************************************************************************/

    private const DhtNode node;


    /***************************************************************************

        SIGINT, SIGTERM and SIGQUIT handler event

    ***************************************************************************/

    private const SignalEvent sigint_event;


    /***************************************************************************

        Periodic processes manager

    ***************************************************************************/

    protected const Periodics periodics;


    /***************************************************************************

        Storage channels owned by the node (created by sub-class).

    ***************************************************************************/

    protected DhtStorageChannels storage_channels;


    /***************************************************************************

        Minimum and maximum hashes for which this node is responsible.

    ***************************************************************************/

    protected const hash_t min_hash, max_hash;


    /***************************************************************************

        Constructor

    ***************************************************************************/

    public this ( ServerConfig server_config, ConfigParser config )
    {
        this.server_config = server_config;

        this.epoll = new EpollSelectDispatcher;

        ConfigReader.fill("Stats", this.stats_config, config);
        ConfigReader.fill("Performance", this.performance_config, config);

        this.sigint_event = new SignalEvent(&this.sigintHandler,
            [SIGINT, SIGTERM, SIGQUIT]);

        assertEx(Hash.hashDigestToHashT(this.server_config.minval(),
            this.min_hash, true),
            "Minimum hash specified in config file is invalid -- "
            "a full-length hash is expected");

        assertEx(Hash.hashDigestToHashT(this.server_config.maxval(),
            this.max_hash, true),
            "Maximum hash specified in config file is invalid -- "
            "a full-length hash is expected");

        this.node = new DhtNode(this.node_item, this.newStorageChannels(),
            this.min_hash, this.max_hash, this.epoll, server_config.backlog);

        this.node.error_callback = &this.nodeError;
        this.node.connection_limit = server_config.connection_limit;

        this.periodics = new Periodics(this.node, this.epoll);
        this.periodics.add(new PeriodicWriterFlush(
            this.epoll, this.performance_config.write_flush_ms));
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
        // FIXME: any errors which occur after the sigintHandler() has exited
        // are just printed to the console. This is a hack to work around an
        // unknown compiler bug which causes segfaults inside the tango logger
        // (apparently something to do with variadic args) due to the ptr and
        // length of an array being swapped in the arguments list of a function.
        // We need to investigate this properly and try to work out what the bug
        // is.
        if ( Terminator.shutdown )
        {
            Stderr.formatln("Node error: " ~ exception.msg);
            return;
        }

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
        this.node.stopListener(this.epoll);
        log.trace("SIGINT handler: stopping node listener finished");

        this.shutdown();

        log.trace("SIGINT handler: shutting down node");
        this.node.shutdown();
        log.trace("SIGINT handler: shutting down node finished");

        log.trace("SIGINT handler: shutting down epoll");
        this.epoll.shutdown();
        log.trace("SIGINT handler: shutting down epoll finished");

        log.trace("Finished SIGINT handler");

        Terminator.shutdown = true;
    }


    /***************************************************************************

        Node shutdown hook. Base class implementation does nothing, but derived
        classes may override to add their own special behaviour here.

    ***************************************************************************/

    protected void shutdown ( ) { }
}

