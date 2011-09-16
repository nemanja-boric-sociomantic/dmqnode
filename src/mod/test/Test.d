/*******************************************************************************

    Queue test class 

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.Test;

/*******************************************************************************

    Internal Imports

*******************************************************************************/

private import src.mod.test.writeTests.WriteTests;

/*******************************************************************************

    Swarm Imports

*******************************************************************************/

private import swarm.queue.QueueClient,
               swarm.queue.QueueConst;

/*******************************************************************************

    Ocean Imports

*******************************************************************************/

private import ocean.text.Arguments,
               ocean.io.select.EpollSelectDispatcher,
               ocean.util.log.Trace;

/*******************************************************************************

    Tango Imports

*******************************************************************************/

private import Integer = tango.text.convert.Integer;

private import tango.core.Thread,
               tango.core.sync.Barrier,
               tango.io.Stdout,
               tango.util.log.Log,
               tango.core.Array : contains;

/*******************************************************************************

    Test class
    
    The test class runs one set of tests for each of the commands:
    
    * push
    * pushCompressed
    * pushMulti
    * pushMultiCompressed
    
    It runs each command one time in combination with pop and with consume.
    
    Those combinations are called command tests. They are as follows:
    
    For each previously listed command:
    
    * keep pushing x entries, keep consuming at the same time
    * keep pushing x entries, keep popping at the same time
    * keep pushing till full, then consuming till empty
    * keep pushing till full, then popping till empty     

*******************************************************************************/

class Test : Thread
{  
    /***************************************************************************

        Path to configuration file

    ***************************************************************************/

    char[] config;
    
    /***************************************************************************

        Epoll Select Dispatcher

    ***************************************************************************/

    EpollSelectDispatcher epoll;
        
    /***************************************************************************
    
        QueueClient instance
    
    ***************************************************************************/
    
    QueueClient queue_client;
        
    /***************************************************************************
    
        Arguments instance
    
    ***************************************************************************/
    
    Arguments args;
        
    /***************************************************************************
    
        
    
    ***************************************************************************/
    
    WriteTests write_tests;
      
    /***************************************************************************
    
        Barrier to synchronize with other threads
    
    ***************************************************************************/
    
    Barrier barrier;
    
    /***************************************************************************
    
        Logger instance
    
    ***************************************************************************/
    
    Logger logger;
    
    /***************************************************************************
    
        Constructor
        
        Params:
            args       = arguments to test
            write_test = write_test instance to use 
                         to communicate with the queue
            barrier    = barrier that is shared with other threads using
                         the same channel/ICommand instances
    
    ***************************************************************************/
        
    this ( Arguments args, WriteTests write_tests, Barrier barrier = null ) 
    {   
        this.logger = Log.lookup("Thread(" ~ 
                                 Integer.toString(cast(size_t)cast(void*)this) ~ 
                                 ")");
        
        this.epoll  = new EpollSelectDispatcher;
                
        this.queue_client = new QueueClient(epoll, 1);
        this.queue_client.addNodes(args("config").assigned[0]);     
        
        this.args = args;
        
        this.config = args("config").assigned[0];
        
        this.write_tests = write_tests;
    
        this.barrier = barrier;
        
        super(&this.run);
    }
        
    /***************************************************************************
    
        Runs the tests
    
    ***************************************************************************/
        
    public void run ()
    {
        scope (exit) logger.trace("Thread exited"); 
        foreach (write_test; write_tests) 
        {
            logger.info("Testing {}:", write_test.writeCommandName());
            
            foreach (command; this.args("commands").assigned) 
            {   
                size_t remaining = write_test.getChannelSize(this.epoll, this.queue_client);
                
                if (remaining > 0)
                {
                    try write_test.pop(epoll, queue_client, remaining);
                    catch (EmptyQueueException) { }
                                        
                    if (write_test.getChannelSize(this.epoll, this.queue_client) > 0)
                    {
                        throw new Exception("Could not empty queue test channels for testing");
                    }
                }
                
                if (this.barrier !is null) this.barrier.wait();
                
                switch (command)
                {
                    case "consumer":
                        consumerCommand(write_test);
                        break;
                    case "popper":
                        popperCommand(write_test);
                        break;
                    case "fillPop":
                        fillPopCommand(write_test);
                        break;
                }
            }
        }
    }
      
    /***************************************************************************
    
        Runs the popperCommand test which consists in pushing x entries
        and popping them after each push command.
          
        Params:
            write_test = write_test instance to use 
                         to communicate with the queue
    
    ***************************************************************************/
        
    private void popperCommand ( IWriteTest write_test )
    {
        logger.info("\tpushing and popping items ...");

        for (size_t i = 0; i < 1_00; ++i) with (write_test)
        {
            //this.logger.trace("pushing 5");
            push(this.epoll, this.queue_client, 5);
            //this.logger.trace("popping 5");
            pop(this.epoll, this.queue_client, 5);                
        }
        
        if (this.barrier !is null) this.barrier.wait();
        
        write_tests.finish();        
    }
          
    /***************************************************************************
    
        Runs the consumerCommand test which consists in pushing x entries
        and consuming them at the same time
          
        Params:
            write_test = write_test instance to use 
                         to communicate with the queue
    
    ***************************************************************************/
        
    private void consumerCommand ( IWriteTest write_test )
    {
        logger.info("\tpushing and consuming items ...");
  
        scope Consumer consumer;
        
        try 
        {
            try 
            {
                consumer = new Consumer(this.config, write_test);
                consumer.start;
                
                for ( size_t i = 0; i < 1_000; ++i )
                {
                    write_test.push(this.epoll, this.queue_client, 2);
                    
                    if ( consumer.isRunning == false ) break;
                }
            }
            finally if ( this.barrier !is null ) this.barrier.wait();
            
            this.waitForItems(write_test);
        }  
        finally if ( consumer !is null && consumer.isRunning )
        {
            consumer.stopConsume();
            write_test.push(this.epoll, this.queue_client, 1);
        }
        
        consumer.join(false);
                    
        write_tests.finish();        
    }

    /***************************************************************************

        Blocks till no items are left in the remote queue and according to the
        local counter. Throws if the amount of items/counter doesn't change
        within one second of waiting.
          
        Params:
            write_test = write_test instance to use 
                         to communicate with the queue

    ***************************************************************************/
          
    void waitForItems ( IWriteTest write_test )
    {
        size_t remaining, last;
        
        while ( 0 < (remaining = write_test.getChannelSize(this.epoll, 
                                                             this.queue_client)) )
        {
            logger.info("Waiting for consumer to consume {} bytes in the queue node", 
                        remaining);
            
            if (remaining == last) throw new Exception("Consumer stopped consuming");
                
            last = remaining;
            
            this.sleep(1);
        }
        
        last = 0;
        
        while ( 0 < (remaining = this.write_tests.itemsLeft) )
        {
            logger.info("Waiting for consumer to process {} items", 
                        remaining);
            
            if (remaining == last) 
            {
                logger.error("Consumer did not consume anything the last"
                             " second ({} items left)", remaining);
                
                throw new Exception("Consumer stopped processing");
            }
            
            last = remaining;
            
            this.sleep(1);
        }       
    }
    
    /***************************************************************************
    
        Runs the fillPopCommand test which consists in pushing x entries
        until the queue node tells us "OutOfMemory" and then popping 
        them until all are popped.
          
        Params:
            write_test = write_test instance to use 
                         to communicate with the queue
                         
    ***************************************************************************/
        
    private void fillPopCommand ( IWriteTest write_test )
    {
        logger.info("\tpushing items till full, popping till empty ...");
          
        try do write_test.push(this.epoll, this.queue_client, 5);
        while (write_test.info.status != QueueConst.Status.OutOfMemory)
        catch (UnexpectedResultException e) 
        {
            logger.info("caught exception: {}", e.msg);
            if (e.result != QueueConst.Status.OutOfMemory)
            {
                throw e;
            }
        }
        
        logger.info("now popping");
                    
        try do write_test.pop(this.epoll, this.queue_client, 2);            
        while (this.write_tests.itemsLeft > 0)
        catch (EmptyQueueException e) {}
                    
        if (this.barrier !is null) this.barrier.wait();
           
        auto remained = write_test.getChannelSize(this.epoll, this.queue_client); 
        if (remained != 0)
        {
            logger.error("Not all items where popped. {} items remained",
                         remained);
            
            throw new Exception("Not all items where popped!");
        }
        
        write_tests.finish();        
    }
          
    /***************************************************************************
    
        Runs the fillPopCommand test which consists in pushing x entries
        until the queue node tells us "OutOfMemory" and then consuming 
        them until all are consumed        
        
        Params:
            write_test = write_test instance to use 
                         to communicate with the queue
    
    ***************************************************************************/
        
    private void fillConsumeComand ( IWriteTest write_test )
    {
   /+     logger.info("\tpushing items till full, consuming till empty ...");
        
        with (write_test) try do  push(this.epoll, this.queue_client, 10 );
        while (info.status != QueueConst.Status.OutOfMemory)
        catch (UnexpectedResultException e) 
        {
            if (e.result != QueueConst.Status.OutOfMemory)
            {
                throw e;
            }
        }
        
        write_test.consume(this.epoll, this.queue_client);            
        write_tests.finish();+/
    }
}
/*******************************************************************************

    Consumer thread class. Used to run a consumer without blocking

*******************************************************************************/

class Consumer : Thread
{
    /***************************************************************************

        Logger instance

    ***************************************************************************/
    
    Logger logger;
    
    /***************************************************************************

        Epoll Select Dispatcher

    ***************************************************************************/

    EpollSelectDispatcher epoll;
        
    /***************************************************************************
    
        QueueClient instance
    
    ***************************************************************************/
    
    QueueClient queue_client;
    
    IWriteTest write_test;
    
    /***************************************************************************
    
        Constructor
        
        Params:
            config = configuration file
            push_command = push command instance to be used for consuming
                
    ***************************************************************************/
        
    this ( char[] config, IWriteTest write_test )
    {       
        this.epoll  = new EpollSelectDispatcher;
                
        this.queue_client = new QueueClient(epoll, 10);
        this.queue_client.addNodes(config);
        
        this.write_test = write_test;
        
        super(&this.run);
        
        this.logger = Log.lookup("ConsumerThread");
    }
    
    void run ( )
    {        
        logger.trace("running...");
        scope (exit) logger.trace("exiting ...");
        
        try this.write_test.consume(this.epoll, this.queue_client);
        catch (Exception e) logger.error("Consumer Exception: {}", e.msg);
    }
    
    public void stopConsume ( )
    {
        this.write_test.stopConsume;
        //this.epoll.shutdown;
    }    
}
    
