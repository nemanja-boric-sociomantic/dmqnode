/*******************************************************************************

    Memory commands test class

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.tests.MemoryCommands;

/*******************************************************************************

    Internal Imports

*******************************************************************************/

private import src.mod.test.tests.Commands;
           
/*******************************************************************************

    Swarm Imports

*******************************************************************************/
 
private import swarm.dht.DhtClient;

/*******************************************************************************

    Ocean Imports

*******************************************************************************/

private import ocean.io.select.EpollSelectDispatcher,
               ocean.io.digest.Fnv1,
               ocean.util.log.SimpleLayout;

/*******************************************************************************

    Tango Imports

*******************************************************************************/

private import tango.core.Thread,
               tango.util.log.Log,
               tango.util.container.HashSet;

private import tango.core.Memory;

private import Integer = tango.text.convert.Integer;

/*******************************************************************************

    Test class for the memory node commands

*******************************************************************************/

class MemoryCommands : Commands
{
    /***************************************************************************

        Unspectacular Constructor
        
        Params:
            connections = amount of connections to use
            config      = path to the xml configuration file

    ***************************************************************************/
    
    this ( size_t connections, char[] config )
    {
        this.logger = Log.lookup("MemoryCommands");
        
        super(connections, config);
    }
    
    /***************************************************************************

        Run a series of tests

    ***************************************************************************/

    override void run()
    {
        this.testRemoveChannel();      
        this.testPut(false);
        this.testRemove();
        this.testPut(false);
        //this.testRemoveChannel();
        //this.testListen(&this.dht.put!(uint));
    }
    
    /***************************************************************************

        Confirm that the remote and local states are the same

    ***************************************************************************/
  
    protected override void confirm ( )
    {        
        this.confirmGetAll();  
        this.confirmGet();
        this.confirmGetAllKeys();
        this.confirmExists();
        this.confirmChannelSize();
    }  
    
    private:
        
    /***************************************************************************

        Tests the put command
        
        Params:
            compress = whether to put compressed values or not

    ***************************************************************************/

    void testPut ( bool compress )
    {        
        logger.info("Testing put command (writing {}k {}entries)", 
                    Iterations / 1000,
                    compress ? "compressed " : "");
        Exception exception = null;
        ubyte[500] data = 0xCC;
        
        for ( size_t i = 0; i < Iterations; ++i )
        {   
            logger.trace("for begin {}", i);
            char[] putter ( DhtClient.RequestContext )
            {
                logger.trace("before getRandom");
                auto d = cast(char[]) this.getRandom(data, i);
                logger.trace("put: {}", cast(ubyte[])d);
                
                return d;
            }
            
            with (this.dht) assign(put(channel, i, &putter, &this.requestNotifier)
                                   .compress(compress));        
            
            logger.trace("running req");
            this.runRequest(exception);
            
            logger.trace("adding val");
            this.values.add(i);
        }        
        
        this.confirm();
    }   

    /***************************************************************************

        Tests the remove command

    ***************************************************************************/

    void testRemove ( )
    {
        logger.info("Testing remove command (removing {}k entries)", 
                    this.values.size/1000);
        Exception exception = null;
        ubyte[500] data = void;
        auto num = this.values.size;
        
        for ( size_t i = 0; i < num; ++i )
        {
            with(this.dht) assign(remove(channel, i, &this.requestNotifier));        
                    
            this.runRequest(exception);
            
            this.values.remove(i);
        }        
        
        this.confirm();
    }
    
    /***************************************************************************

        Confirms that the local and remote data is equal by getting each
        local key from the node and validating the value.

    ***************************************************************************/

    void confirmGet ( )
    {
        logger.info("\tconfirming using get");
        Exception exception = null;
                
        foreach (key; this.values)
        {        
            void getter ( DhtClient.RequestContext, char[] value )
            {
                exception = validateValue(key, value);
                
                if ( exception !is null ) throw exception;
            }
            
            with(this.dht) assign(get(channel, key, &getter, &this.requestNotifier));
        
            this.runRequest(exception);
        }
    }  

    /***************************************************************************

        Confirms that the local and remote data is equal by 
        checking the existence of each local key at the dht node

    ***************************************************************************/
 
    void confirmExists ( )
    {
        logger.info("\tconfirming using exists");
        Exception exception = null;
               
        foreach (key; this.values)
        {
            void getter ( DhtClient.RequestContext, bool exists )
            {
                if ( this.values.contains(key) != exists )
                {
                    return new Exception("Key not found", __FILE__, __LINE__);
                }
                
                if ( exception !is null ) throw exception;
            }
            
            with (this.dht) assign(exists(channel, key, &getter, &this.requestNotifier));
        
            this.runRequest(exception);
        }
    }
}