/*******************************************************************************

    Abstract test class

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.tests.Commands;

/*******************************************************************************

    Internal Imports

*******************************************************************************/

private import src.mod.test.tests.Test;

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

public import tango.core.Array,
              tango.core.Thread,
              tango.util.log.Log,
              tango.util.container.HashSet;

private import Integer = tango.text.convert.Integer;

/*******************************************************************************

      Special Exception indicating the interruption of an otherwise
      not interruptable command (e.g. the listen command)

*******************************************************************************/

class EndException : Exception
{
    this ( )
    {
        super("Intended Exception", __FILE__, __LINE__);
    }
}

/*******************************************************************************

        Abstract Test class offering functions to test many commands

*******************************************************************************/

class Commands : Test
{    
    /***************************************************************************

        A local hash set to compare and validate with the remote dht node

    ***************************************************************************/

    protected HashSet!(size_t) values;

    /***************************************************************************

        The channel that will be tested

    ***************************************************************************/

    protected char[] channel = "test_channel";

    /***************************************************************************

        Amount of iterations to do in each test

    ***************************************************************************/

    protected size_t Iterations = 50_000;

    /***************************************************************************

        Constructor
        
        Params:
            connections = amount of connections to use
            config      = path to the xml configuration file

    ***************************************************************************/

    this ( size_t connections, char[] config )
    {
        this.values = new HashSet!(size_t);

        super(connections, config);
    }
    
    protected:

    /***************************************************************************

        Abstract function that should confirm that the local and remote state
        is the same
        
        Params:
            filter = what to filter for in getAll/Range request

    ***************************************************************************/
    
    abstract void confirm ( ubyte[] filter = null );

    /***************************************************************************

        Tests the listen node command

        Params:
            putFunc = address of the function that should be used to put
                      values to the dht node

    ***************************************************************************/

    void testListen ( T ) ( T putFunc, bool compress )
    {
        logger.info("Testing listen command (writing {}k {}entries)",
                    Iterations / 1000, compress ? "compressed " : "");
        
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
            
            if ( ++pushed < Iterations ) with (this.dht)
            {
                logger.trace("triggering put");
                assign(putFunc(channel, pushed, &putter, &this.requestNotifier));
            }
            else throw new EndException;
        }

        with (this.dht)
        {
            /* Note: the Put request is scheduled slightly in the future to
             * avoid the situation where, due to the network, the dht node
             * processes it before the listen request has begun, leading to a
             * record being 'lost'.
             */ 

            assign(listen(channel, &getter, &this.requestNotifier));
            schedule(putFunc(channel, pushed, &putter, &this.requestNotifier), 20);
        }

        this.runRequest(exception);

        //this.confirm(compress ? null : [cast(ubyte)98]);

//        this.confirm(null);
    }

    /***************************************************************************

        Tests the removeChannel node command

    ***************************************************************************/

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

    /***************************************************************************

        confirms that local and remote data is equal by 
        comparing keys and values

    ***************************************************************************/

    void confirmGetAll ( ubyte[] filter = null )
    {       
        logger.info("\tconfirming using getAll{}", 
                    filter.length > 0 ? " (filtered)" : "");
        
        Exception exception = null;
        size_t expecting    = 0;
        ubyte[500] data     = void;
        
        void getter ( DhtClient.RequestContext, char[] key_str, char[] value )
        {    
            if ( key_str.length + value.length == 0 ) return;
                
            auto key = Integer.parse(key_str, 16);
            exception = validateValue(key, value);
            
            if ( exception !is null )
            {
                throw exception;
            }
            else if ( filter.length > 0 )
            {
                if ( expecting == 0 ) 
                {
                    throw exception = new Exception("No more values expected");
                }
                else if ( !value.contains(cast(char[]) filter) ) 
                {
                    throw exception = new Exception("Value should be filtered");
                }
                else
                {
                    --expecting;
                }
            }
        }
        
        auto params = this.dht.getAll(channel, &getter, &this.requestNotifier);
        
        if ( filter.length > 0 ) 
        {
            params.filter(cast(char[]) filter);

            foreach ( key; this.values ) 
            {
                auto value = this.getRandom(data, key);
                
                if ( value.contains(filter) ) expecting++;
            }
        }
        
        this.dht.assign(params);

        logger.trace("Expecting {} results", expecting);
        
        this.runRequest(exception);
        
        if ( expecting != 0 ) throw new Exception("Not all expected results arrived");
    }

    /***************************************************************************

        confirms that local and remote data is equal by comparing the keys

    ***************************************************************************/

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

    /***************************************************************************

        confirms that local and remote data is equal by comparing the amount
        of elements

    ***************************************************************************/

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

    /***************************************************************************

        Validates the given key and value.
        Checks that the key exists locally and that the value is correct
        
        Params:
            key   = key to validate
            value = value to validate 

    ***************************************************************************/

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
    
        Takes a number and creates a chunk of data out of it
        
        Params:
            data = buffer to write result to
            init = number to create data from
             
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
    
    /***************************************************************************

        Runs the eventloop and handles any resulting errors

    ***************************************************************************/

    void runRequest ( ref Exception exception ) 
    {        
        this.epoll.eventLoop;
        
        if ( exception !is null ) throw exception;
        
        if ( this.exception !is null && 
             cast(EndException) this.exception is null )
        {
            throw this.exception;
        }   
    }
}