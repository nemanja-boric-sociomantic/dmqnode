/*******************************************************************************

    Queue Push write test

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.writeTests.IWriteTest;

/*******************************************************************************

    Internal Imports

*******************************************************************************/

public import src.mod.test.writeTests.WriteTests,
              src.mod.test.writeTests.Exceptions;

/*******************************************************************************

    Swarm Imports

*******************************************************************************/

public import swarm.queue.QueueClient,
               swarm.queue.QueueConst,
               swarm.queue.client.request.params.RequestParams;

/*******************************************************************************

    Ocean Imports

*******************************************************************************/

public import ocean.io.select.EpollSelectDispatcher;

private import ocean.io.digest.Fnv1;

/*******************************************************************************

    Tango Imports

*******************************************************************************/

private import tango.math.random.Random;

private import tango.math.random.Random,
               tango.core.sync.Atomic,
               tango.util.log.Log;

/*******************************************************************************

    

*******************************************************************************/

abstract class IWriteTest
{
    public bool stop_consume = false;
    
    /***************************************************************************
    
       
    
    ***************************************************************************/
    
    protected alias void delegate ( uint index, ubyte[] value ) SuccessDG;
                 
    /***************************************************************************
    
        Amount of channels
    
    ***************************************************************************/
    
    protected size_t num_channels;
    
    /***************************************************************************
    
        Logger instance
    
    ***************************************************************************/
    
    protected Logger logger;
    
    /***************************************************************************
    
        Status of the last request
        
    ***************************************************************************/
    
    QueueConst.Status.BaseType status;
    
    /***************************************************************************
    
        
        
    ***************************************************************************/
       
    protected WriteTests write_tests;
    
    /***************************************************************************
    
        
        
    ***************************************************************************/
    
    this ( WriteTests write_tests, size_t num_channels = 1 )
    {
        this.num_channels = num_channels;
        this.write_tests = write_tests;
        this.logger = Log.lookup("WriteTest." ~ this.writeCommandName() ~ 
                         "[" ~ Integer.toString(write_tests.instance_number) ~ "]");
        
        this.logger.trace("setup");
    }
            
    /***************************************************************************
    
        
        
    ***************************************************************************/
    
    size_t getChannelSize ( EpollSelectDispatcher epoll, QueueClient queue_client );
    
    /***************************************************************************
    
        Pushes a test entry to the remote queue and increases the local counter
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
    ***************************************************************************/
    
    void push ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                size_t amount = 1, 
                QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok );
        
    /***************************************************************************
    
        Pops an entry from the remote queue and compares the result with the 
        local item array.
        Throws if the values don't match.
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
         Returns:
             amount of popped entries
             
    ***************************************************************************/
    
    size_t pop ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                 size_t amount = 1, 
                 QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok );
        
    /***************************************************************************
    
        Consumes entries from the remote queue and compares them with the values
        in the local items array.
        Throws if the values don't match.
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            expected_result = optional, expected result code, defaults to Ok            
             
    ***************************************************************************/
    
    void consume ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                   QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok );

       
    /***************************************************************************
    
        request finished dg. Sets the status.
             
    ***************************************************************************/
    
    protected void requestFinished ( QueueClient.RequestNotification info )
    {
        if ( info.type == info.type.Finished )
        {
            this.status = info.status;
        }
    }   
    
    /***************************************************************************
    
        creates a random amount of bytes
             
    ***************************************************************************/
    
    protected ubyte[] getRandom ( ubyte[] data, uint init )
    {
        uint i = Fnv1(init) % (this.write_tests.max_item_size - uint.sizeof) + 1;
 
        data[0 .. uint.sizeof] = (cast(ubyte*)&init) [0 .. uint.sizeof];
        
        foreach (ref b; data[uint.sizeof .. i + uint.sizeof]) 
        {
            b = Fnv1(++init) ;
        }
       
        return data[0 .. i + uint.sizeof];
    }
            
    /***************************************************************************
    
        
        
    ***************************************************************************/
    
    synchronized protected CommandsException validateValue ( SuccessDG success, 
                                                             char[] value,
                                                             char[] file, 
                                                             size_t line )
    {
        ubyte[] data = new ubyte[this.write_tests.max_item_size];
        ubyte[] gdata;
        
        if ( value.length > uint.sizeof )
        {                        
            uint num = *(cast(uint*)value.ptr);
            
            gdata = this.getRandom(data, num);
            
            if ( num >= this.write_tests.items.length )
            {
                // Value refers to an index that we don't have
                return new InvalidValueException(cast(ubyte[]) value,
                                                 gdata, 
                                                 this.write_tests.items.length,
                                                 file, line);
            }
            else if ( gdata != cast(ubyte[]) value )
            {
                // Generated data is not the same as data in the value
                return new InvalidValueException(cast(ubyte[]) value,
                                                 gdata, 
                                                 this.write_tests.items.length,
                                                 file, line);
            }
            else if ( this.write_tests.items[num] == 0 )
            {
                // Value was already read or never sent
                logger.trace("Received: {}", cast(ubyte[]) value);
                return new InconsistencyException(num, file, line);
            }
            else
            {
                
                // Everything is fine
                success(num, cast(ubyte[]) value);
                
                return null;
            }
            
        }
        else if ( value.length == 0 )
        {
            // Empty response
            return new EmptyQueueException(file, line);
        }
        else
        {
            // Value is too short
            return new InvalidValueException(cast(ubyte[]) value,
                                             null, 
                                             this.write_tests.items.length,
                                             file, line);
        }
    }
         
    abstract protected void doPush ( QueueClient, EpollSelectDispatcher, ubyte[] );
        
    /***************************************************************************
    
        
        
    ***************************************************************************/
    
    protected void push ( EpollSelectDispatcher epoll, 
                          QueueClient queue_client, size_t amount, 
                          QueueConst.Status.BaseType expected_result )
    {
        ubyte[] data = new ubyte[this.write_tests.max_item_size];
        
        do synchronized (this)
        {
            auto rdata = getRandom(data, this.write_tests.push_counter);

            this.doPush(queue_client, epoll, rdata);

            epoll.eventLoop;
            
            if (this.status != expected_result)
            {                
                this.logger.info("unexpected result");
                throw new UnexpectedResultException(this.status, 
                                                    expected_result,
                                                    __FILE__, __LINE__);
            }       
            
            this.write_tests.items[this.write_tests.push_counter++] += this.num_channels;
        }
        while (--amount > 0) 
    }      
    
    /***************************************************************************
    
        Name of the command used for writing
        
        Returns:
            Name of the command used for writing
             
    ***************************************************************************/
    
    char[] writeCommandName ();  
    
    final void stopConsume ( )
    {
        logger.info("stopping consume");
        atomicStore(this.stop_consume, true);
        
        assert(this.stop_consume == true, "uh?");
    }
}