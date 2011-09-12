/*******************************************************************************

    Queue Push write test

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.writeTests.Produce;

private import src.mod.test.writeTests.IWriteTest,
               src.mod.test.writeTests.WriteTests;


class Produce : IWriteTest
{     
    /***************************************************************************
    
        Constructor
        
        Params:
            size   = size of the local test array in bytes
    
    ***************************************************************************/
    
    this ( WriteTests write_tests )
    {        
        super(write_tests);
    }
           
    override size_t getChannelSize ( EpollSelectDispatcher epoll, QueueClient queue_client )
    {
        size_t size;
        void receiver ( QueueClient.RequestContext, char[], ushort, char[], 
                        ulong records, ulong bytes )
        {
            size += bytes;
        }
        
        with(queue_client) assign(getChannelSize(this.write_tests.channel, 
                                                 &receiver, 
                                                 &this.requestFinished));
        
        epoll.eventLoop;
        
        return size;
    }
    
    protected void doPush ( QueueClient queue_client, 
                            EpollSelectDispatcher epoll, ubyte[] data )
    {       
        void producer ( QueueClient.RequestContext context, QueueClient.IProducer producer )
        {
            producer(cast(char[])data);
            data = [];
        }
        
        with (queue_client) assign(produce(this.write_tests.channel, &producer, 
                                           &requestFinished));
        
        epoll.eventLoop;
    }
    
    /***************************************************************************
    
        Pops an entry from the remote and local queue and compares the result.
        Throws if the values don't match.
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
         Returns:
             amount of popped entries
             
    ***************************************************************************/
    
    override size_t pop ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                          size_t amount = 1, 
                          QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {
        CommandsException exc = null;
        
        do
        {                  
            synchronized (this)
            {              
                void popper ( QueueClient.RequestContext, char[] value )
                {
                    logger.trace("read: {}", cast(ubyte[]) value);
                    exc = this.validateValue((uint num, ubyte[])
                          {
                              this.write_tests.items[num] --;
                              this.write_tests.push_counter --;
                          }, value, __FILE__, __LINE__);
                }
                
                with (queue_client) assign(pop(this.write_tests.channel, &popper, 
                                               &super.requestFinished));            
                
                epoll.eventLoop;
            }
            
            if (info.status != expected_result)
            {
                throw new UnexpectedResultException(info.status, 
                                                    expected_result,
                                                    __FILE__, __LINE__);
            }
            
            if (exc !is null) throw exc;
        }
        while (--amount > 0)
            
        return amount;
    }
        
    /***************************************************************************
    
        Consumes entries from the remote and local queue and compares the results.
        Throws if the values don't match.
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            expected_result = optional, expected result code, defaults to Ok            
             
    ***************************************************************************/
    
    override void consume ( EpollSelectDispatcher epoll, 
                            QueueClient queue_client,
                            QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {
        CommandsException exc = null;
        
        void consumer ( QueueClient.RequestContext, char[] value )
        {   
            logger.trace("consumed: {}", cast(ubyte[])value);
            exc = this.validateValue((uint num, ubyte[])
                  {
                      this.write_tests.items[num] --;
                      this.write_tests.push_counter --;
                  }, value, __FILE__, __LINE__);
        }
        
        with (queue_client) assign(consume(this.write_tests.channel,  
                                           &consumer, &this.requestFinished));
        
        epoll.eventLoop;        
                
        if (info.status != expected_result)
        {
            throw new UnexpectedResultException(info.status, 
                                                expected_result,
                                                __FILE__, __LINE__);
        }
    }   
        
    /***************************************************************************
    
        Name of the command used for writing
        
        Returns:
            Name of the command used for writing
             
    ***************************************************************************/
    
    char[] writeCommandName ( )
    {
        return "produce";
    }
}



class ProduceCompressed : Produce
{           
    
    /***************************************************************************
    
        Constructor
        
        Params:
            size   = size of the local test array in bytes
    
    ***************************************************************************/
    
    this ( WriteTests write_tests )
    {        
        super(write_tests);
    }
         
    override protected void doPush ( QueueClient queue_client, 
                                     EpollSelectDispatcher epoll, ubyte[] data )
    {
        char[] pusher ( QueueClient.RequestContext id )
        {
            return cast(char[]) data;
        }
        
        with (queue_client) assign(push(this.write_tests.channel, &pusher, 
                                        &requestFinished).compressed);
        
        epoll.eventLoop;
    }
    
    /***************************************************************************
    
        Name of the command used for writing
        
        Returns:
            Name of the command used for writing
             
    ***************************************************************************/
    
    char[] writeCommandName ( )
    {
        return "produceCompressed";
    }
}