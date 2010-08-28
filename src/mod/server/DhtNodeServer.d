/*******************************************************************************

    Inovkes Queue Daemon
    
    copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved
    
    version:        Jun 2009: Initial release
    
    authors:        Thomas Nicolai & Lars Kirchhoff


********************************************************************************/

module  mod.server.DhtNodeServer;

/*******************************************************************************

    Imports 

******************************************************************************/

private import mod.server.DhtDaemon;

private import ocean.sys.Daemon;
private import ocean.sys.SignalHandler;

debug private import tango.util.log.Trace;

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
    
    static DhtDaemon dht;
    
    /**
     * Queue Server Daemon
     *
     * Returns:
     *     false, if help message should be printed to Stdout
     */
    public static bool run ( )
    {
        this.dht = new DhtDaemon();
        return !!this.dht.run();
    }
    
    static this ( )
    {
        SignalHandler.set(this.Signals, &shutdown);
    }
    
    extern (C) private static void shutdown ( int code )
    {
        debug Trace.formatln('\n' ~ SignalHandler.getId(code));
        
        this.dht.shutdown();
    }
}

