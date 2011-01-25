/*******************************************************************************

    Inovkes Dht Daemon
    
    copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved
    
    version:        Jun 2009: Initial release
    
    authors:        Thomas Nicolai & Lars Kirchhoff

********************************************************************************/

module  mod.server2.DhtNodeServer;

/*******************************************************************************

    Imports 

******************************************************************************/

private import mod.server2.DhtDaemon;

private import ocean.sys.Daemon;
private import ocean.sys.SignalHandler;

debug private import tango.util.log.Trace;

/*******************************************************************************

    Dht Server Module
    
    ---
    
    Code Usage:
    
    ! this() should always be protected
    ! run() should always exist as static method invoking logic
      and return true or false for arguments parser
    
    ---

********************************************************************************/

struct DhtNodeServer
{
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
        SignalHandler.register(SignalHandler.AppTermination, &shutdown);
    }
    
    private static bool shutdown ( int code )
    {
        debug Trace.formatln('\n' ~ SignalHandler.getId(code));
        
        this.dht.shutdown();
        
        return true;
    }
}

