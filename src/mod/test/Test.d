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

private import src.mod.test.Commands;

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
    char[] config;
    
    bool error = false;
    
    /***************************************************************************

        Epoll Select Dispatcher -- owned and managed by this class. The event
        loop is never called by any other class.

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
    
        Array of the push commands that will be tested
    
    ***************************************************************************/
    
    ICommand[] push_variations;
      
    /***************************************************************************
    
        Barrier to synchronize with other threads
    
    ***************************************************************************/
    
    Barrier barrier;
    
    Logger logger;
    
    /***************************************************************************
    
        Constructor
        
        Params:
            args =     arguments to test
            commands = instances of command executers
    
    ***************************************************************************/
        
    this ( Arguments args, ICommand[] push_variations, Barrier barrier = null ) 
    {   
        this.logger = Log.lookup("Thread(" ~ 
                                 Integer.toString(cast(size_t)cast(void*)this) ~ 
                                 ")");
        
        this.epoll  = new EpollSelectDispatcher;
                
        this.queue_client = new QueueClient(epoll, 1);
        this.queue_client.addNodes(args("config").assigned[0]);     
        
        this.args = args;
        
        this.config = args("config").assigned[0];
        
        this.push_variations = push_variations;
    
        this.barrier = barrier;
        
        super(&this.run);
    }
        
    /***************************************************************************
    
        Runs the tests
    
    ***************************************************************************/
        
    public void run ()
    {
        scope (exit) logger.trace("Thread exited"); 
        foreach (push_command; push_variations) 
        {
            logger.info("Testing {}:", push_command.name());
            
            foreach (command; this.args("commands").assigned) 
            {   
                size_t remaining = push_command.getChannelSize(this.epoll, this.queue_client);
                if (remaining > 0)
                {
                    try push_command.pop(epoll, queue_client, remaining);
                    catch (EmptyQueueException) { }
                    
                    
                    
                    if (push_command.getChannelSize(this.epoll, this.queue_client) > 0)
                    {
                        throw new Exception("Could not empty queue test channels for testing");
                    }
                }
                
                if (this.barrier !is null) this.barrier.wait();
                
                switch (command)
                {
                    case "consumer":
                        consumerCommand(push_command);
                        break;
                    case "popper":
                        popperCommand(push_command);
                        break;
                    case "fillPop":
                        fillPopCommand(push_command);
                        break;
                }
            }
        }
    }
      
    /***************************************************************************
    
        Runs the popperCommand test which consists in pushing x entries
        and popping them after each push command.
    
    ***************************************************************************/
        
    private void popperCommand ( ICommand push_command )
    {
        logger.info("\tpushing and popping items ...");

        for (size_t i = 0; i < 10_000; ++i)
        {
            push_command.push(this.epoll, this.queue_client, 5);
            push_command.pop(this.epoll, this.queue_client, 5);                
        }
        
        if (this.barrier !is null) this.barrier.wait();
        
        push_command.finish();        
    }
          
    /***************************************************************************
    
        Runs the consumerCommand test which consists in pushing x entries
        and consuming them at the same time
    
    ***************************************************************************/
        
    private void consumerCommand ( ICommand push_command )
    {
        logger.info("\tpushing and consuming items ...");
  
        Consumer consumer;
        
        try 
        {
            try 
            {
                consumer = new Consumer(this.config, push_command);
                consumer.start;
                
                for ( size_t i = 0; i < 10_000; ++i )
                {
                    push_command.push(this.epoll, this.queue_client, 2);
                    
                    if ( consumer.isRunning == false ) break;
                }
            }
            finally  if ( this.barrier !is null ) this.barrier.wait();
            
            this.waitForItems(push_command);
            
            if ( consumer !is null && consumer.isRunning )
            {
                consumer.stopConsume();
            }
        }  
        finally consumer.stopConsume();
        
        consumer.join(false);
                    
        push_command.finish();        
    }
          
    void waitForItems ( ICommand push_command )
    {
        size_t remaining, last;
        
        while ( 0 < (remaining = push_command.getChannelSize(this.epoll, 
                                                             this.queue_client)) )
        {
            logger.info("Waiting for consumer to consume {} bytes in the queue node", 
                        remaining);
            
            if (remaining == last) throw new Exception("Consumer stopped consuming");
                
            last = remaining;
            
            this.sleep(1);
        }
        
        last = 0;
        
        while ( 0 < (remaining = push_command.itemsLeft) )
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
    
    ***************************************************************************/
        
    private void fillPopCommand ( ICommand push_command )
    {
        logger.info("\tpushing items till full, popping till empty ...");
          
        try do push_command.push(this.epoll, this.queue_client, 5);
        while (push_command.info.status != QueueConst.Status.OutOfMemory)
        catch (UnexpectedResultException e) 
        {
            if (e.result != QueueConst.Status.OutOfMemory)
            {
                throw e;
            }
        }
                    
        try do push_command.pop(this.epoll, this.queue_client, 2);            
        while (push_command.itemsLeft > 0)
        catch (EmptyQueueException e) {}
                    
        if (this.barrier !is null) this.barrier.wait();
           
        auto remained = push_command.getChannelSize(this.epoll, this.queue_client); 
        if (remained != 0)
        {
            logger.error("Not all items where popped. {} items remained",
                         remained);
            
            throw new Exception("Not all items where popped!");
        }
        
        push_command.finish();
        
    }
          
    /***************************************************************************
    
        Runs the fillPopCommand test which consists in pushing x entries
        until the queue node tells us "OutOfMemory" and then consuming 
        them until all are consumed
        
        TODO: currently no way of knowing when the consumer is done.
    
    ***************************************************************************/
        
    private void fillConsumeComand ( ICommand push_command )
    {
        logger.info("\tpushing items till full, consuming till empty ...");
        
        try do  push_command.push(this.epoll, this.queue_client, 10 );
        while (push_command.info.status != QueueConst.Status.OutOfMemory)
        catch (UnexpectedResultException e) 
        {
            if (e.result != QueueConst.Status.OutOfMemory)
            {
                throw e;
            }
        }
        
        push_command.consume(this.epoll, this.queue_client);
        
        push_command.finish();        
    }
}

class Consumer : Thread
{
    Logger logger;
    
    /***************************************************************************

        Epoll Select Dispatcher -- owned and managed by this class. The event
        loop is never called by any other class.

    ***************************************************************************/

    EpollSelectDispatcher epoll;
        
    /***************************************************************************
    
        QueueClient instance
    
    ***************************************************************************/
    
    QueueClient queue_client;
    
    ICommand push_command;
    
    /***************************************************************************
    
        
    
    ***************************************************************************/
        
    this ( char[] config, ICommand push_command )
    {       
        this.epoll  = new EpollSelectDispatcher;
                
        this.queue_client = new QueueClient(epoll, 10);
        this.queue_client.addNodes(config);
        
        this.push_command = push_command;
        
        super(&this.run);
        
        this.logger = Log.lookup("ConsumerThread");
    }
    
    void run ( )
    {        
        logger.trace("running...");
        scope (exit) logger.trace("exiting ...");
        
        try this.push_command.consume(this.epoll, this.queue_client);
        catch (Exception e) logger.error("Consumer Exception: {}", e.msg);
    }
    
    public void stopConsume ( )
    {
        this.epoll.shutdown;
    }    
}
    
