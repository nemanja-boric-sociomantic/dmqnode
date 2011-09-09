/*******************************************************************************

    Queue Push write test

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.writeTests.PushMulti;

private import src.mod.test.writeTests.IWriteTest,
               src.mod.test.writeTests.WriteTests;

class PushMulti : IWriteTest
{   
    protected size_t multi_responses;
    
    
    /***************************************************************************
    
        Amount of channels
    
    ***************************************************************************/
    
    protected size_t num_channels;
    
    /***************************************************************************
    
        Constructor
        
        Params:
            size   = size of the local test queue in bytes
    
    ***************************************************************************/
    
    this ( WriteTests write_tests , size_t num_channels )
    {
        this.num_channels = num_channels;
        super(write_tests);
    }
                
    /***************************************************************************
    
        
            
    ***************************************************************************/
    
    size_t getChannelSize ( EpollSelectDispatcher epoll, QueueClient queue_client )
    {
        QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok;
        size_t size;
        
        void receiver ( uint, char[], ushort, char[], ulong, ulong bytes )
        {
            size += bytes;
        }

        queue_client.requestFinishedCallback(&this.requestFinished);
        
        for (size_t i = 0; i < this.num_channels; ++i)
        { 
            char[] chan = this.write_tests.channel ~ "_" ~ Integer.toString(i);
                        
            queue_client.getChannelSize(chan, &receiver);
           
            epoll.eventLoop;

            if (info.status != expected_result)
            {
                throw new UnexpectedResultException(info.status, 
                                                    expected_result,
                                                    __FILE__, __LINE__);
            }
        }
        
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
        scope char[][] channels = new char[][num_channels];
        
        foreach (i, ref chan; channels)
        {
            chan = this.write_tests.channel ~ "_" ~ Integer.toString(i);
        }
        
        QueueClient pushFunc ( char[] channel , RequestParams.PutValueDg dg, 
                               uint context = 0)
        {
            return queue_client.pushMulti(channels, dg, context);
        }
        
        super.pushImpl(&pushFunc, epoll, queue_client, amount, expected_result, 
                       num_channels);
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
                
        synchronized (this) do 
        {
            for ( size_t i = 0; i < this.num_channels; ++i, exc = null )
            { 
                char[] chan = this.write_tests.channel ~ "_" ~ Integer.toString(i);
                           
                void popper ( uint, char[] value )
                {
                    exc = this.validateValue((uint num, ubyte[])
                          {
                              this.write_tests.items[num] --;
                          }, value, __FILE__, __LINE__);                    
                }
                
                queue_client.pop(chan, &popper);            
                
                epoll.eventLoop;
                                
                if (info.status != expected_result)
                {
                    throw new UnexpectedResultException(info.status, 
                                                        expected_result,
                                                        __FILE__, __LINE__);
                }                
                
                if (exc !is null) throw exc;
            }
            
            this.write_tests.push_counter --;
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
        queue_client.requestFinishedCallback(&this.requestFinished);
        CommandsException exc = null;
        uint c = 0;
        
        void consumer ( uint id, char[] value )
        {
            exc = this.validateValue((uint num, ubyte[])
            {
                this.write_tests.items[num] --;
                this.multi_responses++;
                
                if (this.multi_responses == num_channels)
                {
                    this.write_tests.push_counter --;
                    this.multi_responses = 0;
                }
            }, value, __FILE__, __LINE__); 
                        
            if (exc !is null) epoll.shutdown;
        }
        
        char[] chan = null;
        
        for (size_t i = 0; i < this.num_channels; ++i)
        {      
            chan = this.write_tests.channel ~ "_" ~ Integer.toString(i);
            
            queue_client.consume(chan, 1, &consumer, null, i);
        }
        
        try epoll.eventLoop;
        catch (Exception e) {}
        
        if (exc !is null) throw exc;
        
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
        return "pushMulti";
    }
}


class PushMultiCompressed : PushMulti
{    
    /***************************************************************************
    
        Constructor
        
        Params:
            size   = size of the local test queue in bytes
    
    ***************************************************************************/
    
    this ( WriteTests write_tests , size_t num_channels )
    {        
        super(write_tests, num_channels);
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
        scope char[][] channels = new char[][num_channels];
        foreach (i, ref chan; channels)
        {
            chan = this.write_tests.channel ~ "_" ~ Integer.toString(i);
        }
        
        QueueClient pushFunc ( char[] channel , RequestParams.PutValueDg dg, 
                               uint context = 0)
        {
            return queue_client.pushMultiCompressed(channels, dg, context);
        }
        
        super.pushImpl(&pushFunc, epoll, queue_client, amount, expected_result, 
                       num_channels);
    }
    
    /***************************************************************************
    
        Name of the command used for writing
        
        Returns:
            Name of the command used for writing
             
    ***************************************************************************/
    
    char[] writeCommandName ( )
    {
        return "pushMultiCompressed";
    }
}