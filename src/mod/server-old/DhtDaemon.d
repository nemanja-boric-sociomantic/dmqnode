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
private import  swarm.dht.storage.LogFiles;

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

    alias       DhtNode!(Filesystem)                            FileSystemNode;

    /***************************************************************************

        alias for Memory node 

     **************************************************************************/

    alias       DhtNode!(Memory)                                MemoryNode;

    /***************************************************************************

        alias for Memory node 

     **************************************************************************/

    alias       DhtNode!(LogFiles, size_t)                      LogFilesNode;

    /***************************************************************************

        alias for node item 

     **************************************************************************/

    alias       DhtConst.NodeItem       NodeItem;

    /***************************************************************************

        alias for storage

     **************************************************************************/

    alias       DhtConst.Code           Storage;


    /***************************************************************************

        Btree node object

     **************************************************************************/

    private     IDhtNode                node;

    /***************************************************************************

         Constructor

     **************************************************************************/

    public this ( )
    {
        NodeItem node_item = this.getNodeItemConfiguration();

        this.setLogger();

        uint    number_threads  = Config.get!(uint)("Server", "connection_threads");
        ulong   size_limit      = Config.get!(ulong)("Server", "size_limit");
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

            case DhtConst.Storage.LogFiles :
                this.node = new LogFilesNode    (node_item, number_threads, size_limit, data_dir, this.getLogFilesWriteBuffer());
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

        char[] minval = Config.Char["Server", "minval"];
        char[] maxval = Config.Char["Server", "maxval"];

        node_item.Address  = Config.Char["Server", "address"];
        node_item.Port     = Config.Int["Server", "port"];
        node_item.MinValue = DhtHash.toHashRangeStart(minval);
        node_item.MaxValue = DhtHash.toHashRangeEnd(maxval);

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
        switch (Config.get!(char[])("Server", "storage_engine"))
        {
            case "hashtable" :
                return DhtConst.Storage.HashTable;

            case "btree" : 
                return DhtConst.Storage.Btree;

            case "filesystem" : 
                return DhtConst.Storage.FileSystem;

            case "memory" : 
                return DhtConst.Storage.Memory;

            case "logfiles" :
                return DhtConst.Storage.LogFiles;

            default :
        }

        return DhtConst.Storage.None;
    }

    /***************************************************************************
    
        Reads the Hashtable tune options from configuration, using default
        values for parameters not specified in configuration.
           
     **************************************************************************/

    private Hashtable.TuneOptions getHashTableTuneOptions ()
    {
        char[] compression_mode;

        Hashtable.TuneOptions tune_options;

        Config.get(tune_options.bnum,   "Options_Hashtable",    "bnum");
        Config.get(tune_options.apow,   "Options_Hashtable",    "apow");
        Config.get(tune_options.fpow,   "Options_Hashtable",    "fpow");
        Config.get(compression_mode,    "Options_Hashtable",    "compression_mode");

        switch (compression_mode)
        {
            case "deflate" :    tune_options.opts = tune_options.opts.Deflate;  break;
            case "bzip" :       tune_options.opts = tune_options.opts.Bzip;     break;
            case "tcbs" :       tune_options.opts = tune_options.opts.Tcbs;     break;
            default :           break;
        }

        return tune_options;
    }

    /***************************************************************************

        Reads the Btree tune options from configuration, using default
        values for parameters not specified in configuration.

     **************************************************************************/

    private Btree.TuneOptions getBTreeTuneOptions ()
    {
        char[] compression_mode;

        Btree.TuneOptions tune_options;

        Config.get(tune_options.bnum,   "Options_Btree",        "bnum");
        Config.get(tune_options.apow,   "Options_Btree",        "apow");
        Config.get(tune_options.fpow,   "Options_Btree",        "fpow");
        Config.get(tune_options.lmemb,  "Options_Btree",        "lmemb");
        Config.get(tune_options.nmemb,  "Options_Btree",        "nmemb");
        Config.get(compression_mode,    "Options_Hashtable",    "compression_mode");

        switch (compression_mode)
        {
            case "deflate" :    tune_options.opts = tune_options.opts.Deflate;  break;
            case "bzip" :       tune_options.opts = tune_options.opts.Bzip;     break;
            case "tcbs" :       tune_options.opts = tune_options.opts.Tcbs;     break;
            default :           break;
        }

        return tune_options;
    }

    /***************************************************************************

        Reads the file output write buffer size for the LogFiles storage engine,
        using the default if not specified in configuration.

     **************************************************************************/

    private size_t getLogFilesWriteBuffer ()
    {
        size_t wbs = LogFiles.DefaultWriteBufferSize;

        Config.get(wbs, "Options_LogFiles", "write_buffer_size");

        return wbs;
    }
}