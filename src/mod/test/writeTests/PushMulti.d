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
    
        Constructor
        
        Params:
            size   = size of the local test queue in bytes
    
    ***************************************************************************/
    
    this ( WriteTests write_tests , size_t num_channels )
    {
        super(write_tests, num_channels);
    }
                
    /***************************************************************************
    
        
            
    ***************************************************************************/
    
    size_t getChannelSize ( EpollSelectDispatcher epoll, QueueClient queue_client )
    {
        QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok;
        size_t size;
        
        void receiver ( QueueClient.RequestContext, char[], ushort, char[], ulong, ulong bytes )
        {
            size += bytes;
        }
        
        for (size_t i = 0; i < this.num_channels; ++i)
        { 
            char[] chan = this.write_tests.channel ~ "_" ~ Integer.toString(i);
                        
            with (queue_client) assign(getChannelSize(chan, &receiver, &requestFinished));
           
            epoll.eventLoop;

            if (super.status != expected_result)
            {
                throw new UnexpectedResultException(super.status, 
                                                    expected_result,
                                                    __FILE__, __LINE__);
            }
        }
        
        return size;
    }
    
    override protected void doPush ( QueueClient queue_client, 
                                     EpollSelectDispatcher epoll, ubyte[] data )
    {
        scope char[][] channels = new char[][num_channels];
        foreach (i, ref chan; channels)
        {
            chan = this.write_tests.channel ~ "_" ~ Integer.toString(i);
        }
  
        char[] pusher ( QueueClient.RequestContext id )
        {
            return cast(char[]) data;
        }
        
        with (queue_client) 
        {
            logger.trace("doing pushMulti on {}", channels);
            assign(pushMulti(channels, &pusher, &requestFinished));
        }        
        
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
                
        synchronized (this) do 
        {
            for ( size_t i = 0; i < this.num_channels; ++i, exc = null )
            { 
                char[] chan = this.write_tests.channel ~ "_" ~ Integer.toString(i);
                           
                void popper ( QueueClient.RequestContext, char[] value )
                {
                    exc = this.validateValue((uint num, ubyte[])
                          {
                              this.write_tests.items[num] --;
                          }, value, __FILE__, __LINE__);                    
                }
                
                with(queue_client) assign(pop(chan, &popper, &requestFinished));            
                
                epoll.eventLoop;
                                
                if (super.status != expected_result)
                {
                    throw new UnexpectedResultException(super.status, 
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
        CommandsException exc = null;
        uint c = 0;
        
        void consumer ( QueueClient.RequestContext id, char[] value )
        {
            logger.trace("consumed: {}", cast(ubyte[])value);
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
            
            with (queue_client) assign(consume(chan, &consumer, &requestFinished));
        }
        
        try epoll.eventLoop;
        catch (Exception e) {}
        
        if (exc !is null) throw exc;
        
        if (super.status != expected_result)
        {
            throw new UnexpectedResultException(super.status, 
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
        
    override protected void doPush ( QueueClient queue_client, 
                                     EpollSelectDispatcher epoll, ubyte[] data )
    {
        scope char[][] channels = new char[][num_channels];
        foreach (i, ref chan; channels)
        {
            chan = this.write_tests.channel ~ "_" ~ Integer.toString(i);
        }
  
        char[] pusher ( QueueClient.RequestContext id )
        {
            return cast(char[]) data;
        }
        
        with (queue_client) 
        {
            logger.trace("doing pushMulti on {}", channels);
            assign(pushMulti(channels, &pusher, &requestFinished).compressed);
        }  
        
        epoll.eventLoop;
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