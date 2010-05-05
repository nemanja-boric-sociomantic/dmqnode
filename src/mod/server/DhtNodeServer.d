/*******************************************************************************

    Inovkes Queue Daemon
    
    copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved
    
    version:        Jun 2009: Initial release
    
    authors:        Thomas Nicolai & Lars Kirchhoff


********************************************************************************/

module  mod.server.DhtNodeServer;

private import mod.server.DhtDaemon;

private import ocean.sys.Daemon;
private import ocean.sys.SignalHandler;

private import swarm.dht.storage.Filesystem;
private import swarm.dht.storage.Hashtable;

private import tango.util.log.Trace;

/*******************************************************************************

    Initialize Main Configuration

********************************************************************************/

private import core.config.MainConfig;


/*******************************************************************************

    Queue Server Module
    
    ---
    
    Code Usage:
    
    ! this() should always be protected
    ! run() should always exist as static method invoking logic
      and return true or false for arguments parser
    
    ---

********************************************************************************/

struct DhtNodeServer
{
    static const Signals = [SignalHandler.SIGINT, SignalHandler.SIGTERM];
    
    alias DhtDaemon!(Hashtable)  Daemon;
    //alias DhtDaemon!(Filesystem)  Daemon;
    
    static Daemon dht;
    
    /**
     * Queue Server Daemon
     *
     * Returns:
     *     false, if help message should be printed to Stdout
     */
    public static bool run ( )
    {
        return !!this.dht.run();
    }
    
    static this ( )
    {
        this.dht = new Daemon;
        
        SignalHandler.set(this.Signals, &shutdown);
    }
    
    extern (C) private static void shutdown ( int code )
    {
        Trace.formatln('\n' ~ SignalHandler.getId(code));
        
        this.dht.shutdown();
    }
}

