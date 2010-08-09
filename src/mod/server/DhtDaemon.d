/*******************************************************************************

        DHT Node Server Daemon
    
        copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved
    
        version:        Jun 2009: Initial release
    
        authors:        David Eckardt, Gavin Norman 
                        Thomas Nicolai, Lars Kirchhoff
     
 ******************************************************************************/

module  mod.server.DhtDaemon;


/*******************************************************************************

    Imports

 ******************************************************************************/

private	import  core.config.MainConfig;

private import  swarm.dht.DhtNode;
private import  swarm.dht.DhtHash;

private import  swarm.dht.storage.Hashtable;
private import  swarm.dht.storage.Btree;
private import  swarm.dht.storage.Filesystem;
private import  swarm.dht.storage.Memory;

private	import	swarm.dht.node.model.IDhtNode;

private import	tango.util.log.Log, tango.util.log.AppendConsole;

debug private import tango.util.log.Trace;



/*******************************************************************************

    DhtDaemon

 ******************************************************************************/

class DhtDaemon
{
    /***************************************************************************
    
        alias for Hashtable node 
    
     **************************************************************************/

    alias       DhtNode!(Hashtable, Hashtable.TuneOptions)      HashTableNode;
 
    /***************************************************************************
    
        alias for BTree node 
    
     **************************************************************************/

    alias       DhtNode!(Btree, Btree.TuneOptions)              BTreeNode;
    
    /***************************************************************************
    
        alias for FileSystem node 
    
     **************************************************************************/
    
    alias       DhtNode!(Filesystem) 			FileSystemNode;
    
    /***************************************************************************
    
        alias for Memory node 
    
     **************************************************************************/
        
    alias       DhtNode!(Memory)				MemoryNode;
    
    /***************************************************************************
    
        alias for node item 

     **************************************************************************/

    alias       DhtConst.NodeItem				NodeItem;
    
    /***************************************************************************
    
        alias for storage
    
     **************************************************************************/

    alias       DhtConst.Storage				Storage;
    
    /***************************************************************************
         
         Hashtable node object
     
     **************************************************************************/
    
    private     HashTableNode					hashtable_node;
    
    /***************************************************************************
    
        Btree node object
            
     **************************************************************************/
   
    private 	IDhtNode						node;
    
    

    /***************************************************************************
    
         Constructor

     **************************************************************************/

    public this ( )
    {
        NodeItem node_item = this.getNodeItemConfiguration();
        
        this.setLogger();
        
        uint    number_threads  = Config.get!(uint)("Server", "connection_threads");
        uint 	size_limit		= Config.get!(uint)("Server", "size_limit");
        char[]  data_dir        = Config.get!(char[])("Server", "data_dir");
        
        Storage storage         = this.getStorageConfiguration();
        
        if (storage == DhtConst.Storage.None)
        {
            throw new Exception("Invalid data storage");
        }
            
        switch (storage)
        {
            case DhtConst.Storage.HashTable :
                this.node = new HashTableNode   (node_item, number_threads, size_limit, data_dir, this.getHashTableTuneOptions());
                break;
                
            case DhtConst.Storage.Btree :
                this.node = new BTreeNode       (node_item, number_threads, size_limit, data_dir, this.getBTreeTuneOptions());
                break;
                
            case DhtConst.Storage.FileSystem :
                this.node = new FileSystemNode  (node_item, number_threads, size_limit, data_dir);
                break;
                
            case DhtConst.Storage.Memory :
                this.node = new MemoryNode      (node_item, number_threads, size_limit, data_dir);
                break;
            
            default: 
                this.node = new HashTableNode   (node_item, number_threads, size_limit, data_dir, this.getHashTableTuneOptions());
                break;            
        }
    }
    
    /***************************************************************************
        
        Runs the DHT node
           
     **************************************************************************/

    public int run ()
    {   
        this.node.run();
        this.node.attach();
        
        return true;
    }
    
    /***************************************************************************
    
        Shuts down the DHT node
           
     **************************************************************************/

    public void shutdown ( )
    {
        return this.node.shutdown();
    }    
    
    /***************************************************************************
    
        Reads NodeItem configuration
           
     **************************************************************************/
        
    private NodeItem getNodeItemConfiguration ()
    {
        NodeItem node_item; 
        
        node_item.Address  = Config.getChar("Server", "address");
        node_item.Port     = Config.getInt("Server", "port");
        node_item.MinValue = DhtHash.toHashRangeStart(Config.getChar("Server", "minval"));
        node_item.MaxValue = DhtHash.toHashRangeEnd(Config.getChar("Server", "maxval"));  
        
        return node_item;
    }    

    /***************************************************************************
    
        Set Logger
           
     **************************************************************************/
        
    private void setLogger ()
    {
        auto log = Log.getLogger("dht.persist");        
        log.add(new AppendConsole);
        log.level = Level.Trace;
    }
    
    /***************************************************************************
    
        Get storage configuration
       
     **************************************************************************/
    
    private Storage getStorageConfiguration ()
    {
        char[] storage =  Config.get!(char[])("Server", "storage_engine");
        
        switch (storage)
        {
            case "hashtable" :
                return DhtConst.Storage.HashTable;
                break;
                
            case "btree" : 
                return DhtConst.Storage.Btree;
                break;
                
            case "filesystem" : 
                return DhtConst.Storage.FileSystem;
                break;
                
            case "memory" : 
                return DhtConst.Storage.Memory;
                break;
                
            default :
                return DhtConst.Storage.None;
                break;
        }
        
        return DhtConst.Storage.None;
    }
    
    /***************************************************************************
    
        Reads the Hashtable tune options from configuration, using default
        values for parameters not specified in configuration.
           
     **************************************************************************/

    private Hashtable.TuneOptions getHashTableTuneOptions ()
    {
        Hashtable.TuneOptions tune_options;
        
        Config.get(tune_options.bnum,   "Options_Hashtable",    "bnum");
        Config.get(tune_options.apow,   "Options_Hashtable",    "apow");
        Config.get(tune_options.fpow,   "Options_Hashtable",    "fpow");
        
        return tune_options;
    }
    
    /***************************************************************************
    
        Reads the Btree tune options from configuration, using default
        values for parameters not specified in configuration.
           
     **************************************************************************/
        
    private Btree.TuneOptions getBTreeTuneOptions ()
    {
        Btree.TuneOptions tune_options;
        
        Config.get(tune_options.bnum,   "Options_Btree",        "bnum");
        Config.get(tune_options.apow,   "Options_Btree",        "apow");
        Config.get(tune_options.fpow,   "Options_Btree",        "fpow");
        Config.get(tune_options.lmemb,  "Options_Btree",        "lmemb");
        Config.get(tune_options.nmemb,  "Options_Btree",        "nmemb");
        
        return tune_options;
    }    
}

