/*******************************************************************************

    DHT node test

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        March 2011: Initial release

    authors:        Gavin Norman


*******************************************************************************/

module src.mod.test.tests.Commands;

private import ocean.io.select.EpollSelectDispatcher,
               ocean.io.digest.Fnv1,
               ocean.util.log.SimpleLayout;
            
private import swarm.dht.DhtClientNew;

private import tango.core.Thread,
               tango.util.log.Log,
               tango.util.container.HashSet;


private import Integer = tango.text.convert.Integer;

class EndException : Exception
{
    this ( )
    {
        super("Intended Exception", __FILE__, __LINE__);
    }
}

class Commands
{
    private HashSet!(size_t) values;
    
    private EpollSelectDispatcher epoll;
    
    private DhtClient dht;
    
    private char[] channel = "test_channel";
    
    private DhtClient.RequestNotification info;
    
    private Logger logger;
    
    this ( size_t connections, char[] config )
    {
        this.logger = Log.lookup("Commands");
        this.epoll  = new EpollSelectDispatcher;
        this.dht    = new DhtClient(epoll, connections);
        this.values = new HashSet!(size_t);
        
        Exception exception = null;
        
        void done ( DhtClient.RequestContext, bool success ) 
        {
            if ( !success ) exception = new Exception(info.message);
        }
        
        with (this.dht)
        {
            addNodes(config);
            nodeHandshake(&done, &this.requestNotifier);
        }
        
        this.runRequest(exception);
    }
    
    void run()
    {
        this.testRemoveChannel();
        this.testPut();
        this.testRemove();
        this.testPut();
        this.testRemoveChannel();
        this.testListen();
        
    }
    
    private:
          
    void confirm ( )
    {        
        this.confirmGetAll();  
        this.confirmGet();
        this.confirmGetAllKeys();
        this.confirmExists();
        this.confirmChannelSize();
    }
    
    void testPut ( )
    {
        logger.info("Testing put command (writing 10k entries)");
        Exception exception = null;
        ubyte[500] data = void;
        
        for ( size_t i = 0; i < 1000; ++i )
        {
            char[] putter ( DhtClient.RequestContext )
            {
                return cast(char[]) this.getRandom(data, i);
            }
            
            with (this.dht) assign(put(channel, i, &putter, &this.requestNotifier));        
                    
            this.runRequest(exception);
            
            this.values.add(i);
        }        
        
        this.confirm();
    }

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
    
    void testListen ( )
    {
        logger.info("Testing listen command (adding 10k entries)");
        
        Exception exception = null;
        ubyte[500] data = void;
        
        size_t pushed = 0;
        
        char[] putter ( DhtClient.RequestContext )
        {
            logger.trace("pushing");
            this.values.add(pushed);
            return cast(char[]) this.getRandom(data, pushed);
        }
                
        void getter ( DhtClient.RequestContext, char[] key_str, char[] value )
        {            
            logger.trace("received val");
            auto key = Integer.parse(key_str, 16);
            exception = validateValue(key, value);
            
            if ( exception !is null ) throw exception;
            
            if ( ++pushed < 10_000 ) with (this.dht)
            {
                logger.trace("triggering put");
                assign(put(channel, pushed, &putter, &this.requestNotifier));
            }
            else throw new EndException;
                
        }
        
        with (this.dht) 
        {
            assign(listen(channel, &getter, &this.requestNotifier));
            assign(put(channel, pushed, &putter, &this.requestNotifier));            
        }
                
        this.runRequest(exception);

        this.confirm();
    }
    
    void testRemoveChannel ( )
    {
        logger.info("Testing removeChannel command (removing {})", 
                    this.channel);
        
        Exception exception = null;
       
        with(this.dht) assign(removeChannel(channel, &this.requestNotifier));        
                    
        this.runRequest(exception);
        
        this.values.clear;
           
        this.confirm();
    }

    
    void runRequest ( ref Exception exception ) 
    {        
        this.epoll.eventLoop;
        
        if ( exception !is null ) throw exception;
        
        if ( !this.info.succeeded )
        {
            if ( this.info.exception !is null )
            {
                if ( !is (EndException : typeof(this.info.exception)) )
                {
                    throw this.info.exception;
                }
                else return;
            }
            
            throw new Exception(info.message);
        }
    }
    
    void confirmGetAll ( )
    {       
        logger.info("\tconfirming using getAll");
        Exception exception = null;
        
        void getter ( DhtClient.RequestContext, char[] key_str, char[] value )
        {    
            if ( key_str.length + value.length == 0 ) return;
                
            auto key = Integer.parse(key_str, 16);
            exception = validateValue(key, value);
            
            if ( exception !is null ) throw exception;
        }
        
        with(this.dht) assign(getAll(channel, &getter, &this.requestNotifier));        
                
        this.runRequest(exception);
    }
    
    void confirmGetAllKeys ( )
    {
        logger.info("\tconfirming using getAllKeys");
        Exception exception = null;
        
        void getter ( DhtClient.RequestContext, char[] key_str )
        {
            auto key = Integer.parse(key_str, 16);
            
            if ( !this.values.contains(key) ) if ( exception is null )
            {
                exception = new Exception("Key not found", 
                                                __FILE__, __LINE__);
            }
            
        }
        
        with(this.dht) assign(getAllKeys(channel, &getter, &this.requestNotifier));
                
        this.runRequest(exception);
    }
    
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
    
    void confirmChannelSize ( )
    {
        logger.info("\tconfirming using channelSize");
        Exception exception = null;
        
        size_t sum = 0;
        
        void getter ( DhtClient.RequestContext, char[] node_address, 
                      ushort node_port, char[], ulong records, ulong bytes )
        {
            sum += records;   
        }
        
        with(this.dht) assign(getChannelSize(channel, &getter, &this.requestNotifier));
                
        this.runRequest(exception);
        
        this.logger.trace("local: {} remote: {}", this.values.size, sum);
        
        if ( this.values.size != sum )
        {
            throw exception = new Exception("Size is wrong",
                                            __FILE__, __LINE__);
        }
    }
    
    
    void requestNotifier ( DhtClient.RequestNotification info )
    {
        this.info = info;
        
        logger.trace("info notification type: {}", info.type);
        logger.trace("info exc: {}", info.exception !is null);
        if (info.exception !is null)
            logger.trace("exc: {}", info.exception);
        
        logger.trace("succeeded: {}", info.succeeded);
    }
    
    Exception validateValue ( uint key, char[] value )
    {
        ubyte[500] data = void;
        if ( !this.values.contains(key) )
        {
            logger.error("Not found: {} (value: {})", key, cast(ubyte[]) value);
            return new Exception("Key not found", __FILE__, __LINE__);
        }
        
        if ( this.getRandom(data, key) != cast(ubyte[]) value )
        {
            return new Exception("Value contains invalid data", __FILE__, __LINE__);
        }
        
        return null;
    }
    
    /***************************************************************************
    
        Creates a random amount of bytes
             
    ***************************************************************************/
    
    ubyte[] getRandom ( ubyte[] data, uint init )
    {
        uint i = Fnv1(init) % (500 - uint.sizeof) + 1;
 
        data[0 .. uint.sizeof] = (cast(ubyte*)&init) [0 .. uint.sizeof];
        
        foreach (ref b; data[uint.sizeof .. i + uint.sizeof]) 
        {
            b = Fnv1(++init) ;
        }
       
        return data[0 .. i + uint.sizeof];
    }        
}