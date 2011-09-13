/*******************************************************************************

    DHT node test

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        March 2011: Initial release

    authors:        Gavin Norman


*******************************************************************************/

module src.mod.test.tests.Commands;

private import ocean.io.select.EpollSelectDispatcher;
            
private import swarm.dht.DhtClient;

private import tango.core.Thread,
               tango.util.container.HashSet;


private import Integer = tango.text.convert.Integer;

class Commands
{
    private HashSet!(size_t) values;
    
    private EpollSelectDispatcher epoll;
    
    private DhtClient dht;
    
    private char[] channel = "test_channel";
    
    private DhtClient.RequestFinishedInfo info;
    
    this ( size_t connections, char[] config )
    {
        this.epoll  = new EpollSelectDispatcher;
        this.dht    = new DhtClient(epoll, connections);
        this.values = new HashSet!(size_t);
        
        with (this.dht)
        {
            addNodes(config);
            nodeHandshake();
        }
        
        super(&run);
    }
    
    void run()
    {
        this.testPut();
        
    }
    
    private:
    
    void testPut ( )
    {
        Exception exception = null;
        ubyte[500] data = void;
        
        for ( size_t i = 0; i < 10_000; ++i )
        {
            char[] put ( )
            {
                return cast(char[]) this.getRandom(data, i);
            }
            
            this.dht.Put(channel, key, &put, &this.requestFinished).assign;        
                    
            this.runRequest(exception);
            
            this.values.add(i);
        }        
        
        this.confirm();
    }
    
    void testRemove ( )
    {
        Exception exception = null;
        ubyte[500] data = void;
        
        for ( size_t i = 0; i < this.values.size; ++i )
        {
            this.dht.Remove(channel, i, &this.requestFinished).assign;        
                    
            this.runRequest(exception);
            
            this.values.remove(i);
        }        
        
        this.confirm();
    }
    
    void confirm ( )
    {        
        this.confirmGetAll();        
        this.confirmGetAllKeys();
        this.confirmGet();
        this.confirmExists();
        this.confirmChannelSize();
    }
    
    void runRequest ( ref Exception exception ) 
    {        
        this.epoll.eventLoop;
        
        if ( exception !is null ) throw exception;
        
        if ( !this.info.succeeded ) throw new Exception(info.message);
    }
    
    void confirmGetAll ( )
    {        
        Exception exception = null;
        
        void get ( char[] key_str, char[] value )
        {
            exception = validateValue(key_str, value);
            
            if ( exception !is null ) throw exception;
        }
        
        this.dht.GetAll(channel, &get, &this.requestFinished).assign;        
                
        this.runRequest(exception);
    }
    
    void confirmGetAllKeys ( )
    {
        Exception exception = null;
        
        void get ( char[] key_str )
        {
            auto key = Integer.parse(key_str, 16);
            
            if ( !this.values.contains(key) )
            {
                throw exception = new Exception("Key not found", 
                                                __FILE__, __LINE__);
            }
        }
        
        this.dht.GetAllKeys(channel, &get, &this.requestFinished).assign;
                
        this.runRequest(exception);
    }
    
    void confirmedGet ( )
    {
        Exception exception = null;
        
        void get ( char[] key_str, char[] value )
        {
            exception = validateValue(key_str, value);
            
            if ( exception !is null ) throw exception;
        }
        
        foreach (key; this.values)
        {
            this.dht.Get(channel, key, &get, &this.requestFinished).assign;
        
            this.runRequest(exception);
        }
    }
    
    void confirmedExists ( )
    {
        Exception exception = null;
               
        foreach (key; this.values)
        {
            void get ( bool exists )
            {
                if ( this.values.contains(key) != exists )
                {
                    return new Exception("Key not found", __FILE__, __LINE__);
                }
                
                if ( exception !is null ) throw exception;
            }
            
            this.dht.Get(channel, key, &get, &this.requestFinished).assign;
        
            this.runRequest(exception);
        }
    }
    
    void confirmChannelSize ( )
    {
        Exception exception = null;
        
        size_t sum = 0;
        
        void get ( char[] node_address, ushort node_port, 
                   ulong records, ulong bytes )
        {
            sum += records;   
        }
        
        this.dht.GetChannelSize(channel, &get, &this.requestFinished).assign;
                
        this.runRequest(exception);
        
        if ( this.values.size != records )
        {
            throw exception = new Exception("Size is wrong",
                                            __FILE__, __LINE__);
        }
    }
    
    
    void requestFinished ( DhtClient.RequestFinishedInfo info )
    {
        this.info = info;
    }
    
    Exception validateValue ( char[] key_str, char[] value )
    {
        ubyte[500] data = void;
        auto key = Integer.parse(key_str, 16);
        if ( !this.values.contains(key) )
        {
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
        uint i = Fnv1(init) % (this.max_item_size - uint.sizeof) + 1;
 
        data[0 .. uint.sizeof] = (cast(ubyte*)&init) [0 .. uint.sizeof];
        
        foreach (ref b; data[uint.sizeof .. i + uint.sizeof]) 
        {
            b = Fnv1(++init) ;
        }
       
        return data[0 .. i + uint.sizeof];
    }        
}