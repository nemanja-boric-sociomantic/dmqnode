/*******************************************************************************

    DHT node test

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        March 2011: Initial release

    authors:        Mathias Baumann


*******************************************************************************/

module src.mod.test.tests.MemoryCommands;

/*******************************************************************************

        Notification type

*******************************************************************************/

private import src.mod.test.tests.Test;

/*******************************************************************************

        Notification type

*******************************************************************************/

private import ocean.io.select.EpollSelectDispatcher,
               ocean.io.digest.Fnv1,
               ocean.util.log.SimpleLayout;
           
/*******************************************************************************

        Notification type

*******************************************************************************/
 
private import swarm.dht.DhtClientNew;

/*******************************************************************************

        Notification type

*******************************************************************************/

private import tango.core.Thread,
               tango.util.log.Log,
               tango.util.container.HashSet;

/*******************************************************************************

        Notification type

*******************************************************************************/

private import Integer = tango.text.convert.Integer;

/*******************************************************************************

        Notification type

*******************************************************************************/

class MemoryCommands : Test
{
    /***************************************************************************

        Notification type

    ***************************************************************************/
    
    this ( size_t connections, char[] config )
    {
        this.logger = Log.lookup("MemoryCommands");
        
        super(connections, config);
    }
    
    /***************************************************************************

        Notification type

    ***************************************************************************/

    override void run()
    {
        this.testRemoveChannel();      
        this.testPut(true);
        this.testRemove();
        this.testPut(false);
        this.testRemoveChannel();
        this.testListen(&this.dht.put!(uint));
    }
    
    /***************************************************************************

        Notification type

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

        Notification type

    ***************************************************************************/

    void testPut ( bool compress )
    {
        logger.info("Testing put command (writing {}k {}entries)", 
                    Iterations / 1000,
                    compress ? "compressed " : "");
        Exception exception = null;
        ubyte[500] data = void;
        
        for ( size_t i = 0; i < Iterations; ++i )
        {
            char[] putter ( DhtClient.RequestContext )
            {
                auto d = cast(char[]) this.getRandom(data, i);
                logger.trace("put: {}", d);
                
                return d;
            }
            
            with (this.dht) assign(put(channel, i, &putter, &this.requestNotifier)
                                   .compress(compress));        
                    
            this.runRequest(exception);
            
            this.values.add(i);
        }        
        
        this.confirm();
    }   

    /***************************************************************************

        Notification type

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

        Notification type

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

        Notification type

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