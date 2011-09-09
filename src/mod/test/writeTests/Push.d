/*******************************************************************************

    Queue Push write test

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.writeTests.Push;

private import src.mod.test.writeTests.IWriteTest,
               src.mod.test.writeTests.WriteTests;


class Push : IWriteTest
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
        void receiver ( uint, char[], ushort, char[], ulong records, ulong bytes )
        {
            size += bytes;
        }
        
        queue_client.getChannelSize(this.write_tests.channel, &receiver);
        
        epoll.eventLoop;
        
        return size;
    }
    
    /***************************************************************************
    
        Pushes a test entry to the remote and local queue
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
    ***************************************************************************/
    
    override void push ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                         size_t amount = 1, 
                         QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {
        this.pushImpl(&queue_client.push, epoll, queue_client, amount, 
                            expected_result);
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
        queue_client.requestFinishedCallback(&this.requestFinished);
        
        do
        {                  
            synchronized (this) 
            {              
                void popper ( uint, char[] value )
                {
                    exc = this.validateValue((uint num, ubyte[])
                          {
                              this.write_tests.items[num] --;
                              this.write_tests.push_counter --;
                          }, value, __FILE__, __LINE__);
                }
                
                queue_client.pop(this.write_tests.channel, &popper);            
                
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
        queue_client.requestFinishedCallback(&this.requestFinished);
        
        void consumer ( uint id, char[] value )
        {   
            exc = this.validateValue((uint num, ubyte[])
                  {
                      this.write_tests.items[num] --;
                      this.write_tests.push_counter --;
                  }, value, __FILE__, __LINE__);
        }
        
        queue_client.consume(this.write_tests.channel, 1, &consumer);
        
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
        return "push";
    }
}



class PushCompressed : Push
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
           
    
    /***************************************************************************
    
        Pushes a test entry to the remote and local queue
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
    ***************************************************************************/
    
    override void push ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                         size_t amount = 1, 
                         QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {
        this.pushImpl(&queue_client.pushCompressed, epoll, queue_client, amount, 
                      expected_result);
    }
            
    /***************************************************************************
    
        Name of the command used for writing
        
        Returns:
            Name of the command used for writing
             
    ***************************************************************************/
    
    char[] writeCommandName ( )
    {
        return "pushCompressed";
    }
}