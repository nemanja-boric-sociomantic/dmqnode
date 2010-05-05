/*******************************************************************************

        Queue Server Daemon
    
        copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved
    
        version:        Jun 2009: Initial release
    
        authors:        Thomas Nicolai & Lars Kirchhoff
        

********************************************************************************/

module  mod.server.DhtDaemon;


/*******************************************************************************

    Imports

********************************************************************************/

private     import      core.config.MainConfig;

private     import      swarm.dht.DhtNode;

private     import      swarm.dht.storage.model.Storage;
/*
private     import      swarm.dht.storage.Hashtable;
private     import      swarm.dht.storage.Filesystem;
*/

private     import      tango.util.log.Log, tango.util.log.AppendConsole;


/*******************************************************************************

    QueueDaemon

********************************************************************************/

class DhtDaemon ( S : Storage, Args ... )
{
    private DhtNode!(S, Args)   node;
    
    /***************************************************************************
    
         Constructor

     ***************************************************************************/
    
    public this ( Args args )
    {
        DhtConst.NodeItem item;
        
        item.MinValue = 0x0;
        item.MaxValue = 0xF;
        item.Address  = Config.getChar("Server", "address");
        item.Port     = Config.getInt("Server", "port");

        auto log = Log.getLogger("dht.persist");
        
        log.add(new AppendConsole);
        log.level = Level.Trace;
        
        uint n_threads = DhtConst.CONNTHREADS;
        
        Config.get(n_threads, "Options", "connection_threads");
        
        this.node = new DhtNode!(S, Args)(item, n_threads, "data", args);
    }    
    
    
    /***************************************************************************
        
        Runs the DHT node
           
    ***************************************************************************/

    public int run ()
    {   
        this.node.start();
        this.node.join();
        
        return true;
    }
    
    /***************************************************************************
    
        Shuts down the DHT node
           
    ***************************************************************************/

    void shutdown ( )
    {
        return this.node.shutdown();
    }
    

}

