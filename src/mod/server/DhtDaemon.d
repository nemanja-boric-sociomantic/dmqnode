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

private     import      swarm.dht.storage.Hashtable;
//private     import      swarm.dht.storage.Filesystem;

private     import      tango.util.log.Log, tango.util.log.AppendConsole;

debug private import tango.util.log.Trace;

/******************************************************************************

    DhtDaemon

 ******************************************************************************/

class DhtDaemon
{
    private DhtNode!(Hashtable, Hashtable.TuneOptions)   node;
    
    
    /**************************************************************************
    
         Constructor

     **************************************************************************/
    
    public this ( )
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
        
        this.node = new DhtNode!(Hashtable, Hashtable.TuneOptions)(item, n_threads, "data", this.getTuneOptions());
    }    
    
    
    /**************************************************************************
        
        Runs the DHT node
           
    ***************************************************************************/

    public int run ()
    {   
        this.node.start();
        this.node.join();
        
        return true;
    }
    
    /**************************************************************************
    
        Shuts down the DHT node
           
    ***************************************************************************/

    public void shutdown ( )
    {
        return this.node.shutdown();
    }
    
    /**************************************************************************
    
        Reads the Hashtable tune options from configuration, using default
        values for parameters not specified in configuration.
           
    ***************************************************************************/

    private Hashtable.TuneOptions getTuneOptions ( )
    {
        Hashtable.TuneOptions tune_options;
        
        Config.get(tune_options.bnum, "Options", "bnum");
        Config.get(tune_options.apow, "Options", "apow");
        Config.get(tune_options.fpow, "Options", "fpow");
        
        return tune_options;
    }
}

