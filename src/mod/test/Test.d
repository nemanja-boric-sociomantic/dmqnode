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
    
    ICommand[] commands;
      
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
        
    this ( Arguments args, ICommand[] commands, Barrier barrier = null ) 
    {   
        this.logger = Log.lookup("Thread(" ~ 
                                 Integer.toString(cast(size_t)cast(void*)this) ~ 
                                 ")");
        
        this.epoll  = new EpollSelectDispatcher;
                
        this.queue_client = new QueueClient(epoll, 1);
        this.queue_client.addNodes(args("config").assigned[0]);     
        
        this.args = args;
        
        this.config = args("config").assigned[0];
        
        this.commands = commands;
    
        this.barrier = barrier;
        
        super(&this.run);
    }
        
    /***************************************************************************
    
        Runs the tests
    
    ***************************************************************************/
        
    public void run ()
    {
        scope (exit) logger.trace("Thread exited");
        foreach (command; this.args("commands").assigned) switch (command)
        {
            case "consumer":
                consumerCommand();
                break;
            case "popper":
                popperCommand();
                break;
            case "fillPop":
                fillPopCommand();
                break;
        }
    }
      
    /***************************************************************************
    
        Runs the popperCommand test which consists in pushing x entries
        and popping them after each push command.
    
    ***************************************************************************/
        
    private void popperCommand ( )
    {
        logger.info("Pushing and popping items:");
        foreach (command; commands)
        {            
            logger.info("\t{} ...", command.name());
            
            for (size_t i = 0; i < 10_000; ++i)
            {
                command.push(this.epoll, this.queue_client, 5);
                command.pop(this.epoll, this.queue_client, 5);                
            }
            
            if (this.barrier !is null) this.barrier.wait();
            
            command.finish();
        }
    }
          
    /***************************************************************************
    
        Runs the consumerCommand test which consists in pushing x entries
        and consuming them at the same time
    
    ***************************************************************************/
        
    private void consumerCommand ( )
    {
        logger.info("Pushing and consuming items:");
        foreach (command; commands)
        {   
            logger.info("\t{} ...", command.name());
            Consumer consumer;
         
            scope (failure) if (consumer !is null && consumer.isRunning)
            {
                consumer.stopConsume();
            }
            
            {
                scope (failure) if (this.barrier !is null) this.barrier.wait();
                
                consumer = new Consumer(this.config, command);
                consumer.start;
                
                for (size_t i = 0; i < 10_000; ++i)
                {
                    command.push(this.epoll, this.queue_client, 2);
                    
                    if (consumer.isRunning == false) break;
                }
            }
    
            size_t remaining, last;
            
            if (this.barrier !is null) this.barrier.wait();
            
            while (0 < (remaining = command.getChannelSize(this.epoll, 
                                                           this.queue_client)))
            {
                logger.info("Waiting for consumer to consume {} bytes in the queue node", 
                            remaining);
                
                if (remaining == last) throw new Exception("Consumer stopped consuming");
                    
                last = remaining;
                
                this.sleep(1);
            }
            
            last = 0;
            
            while (0 < (remaining = command.itemsLeft))
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
            
            consumer.stopConsume();
            consumer.join(false);
                        
            command.finish();
        }
    }
          
    /***************************************************************************
    
        Runs the fillPopCommand test which consists in pushing x entries
        until the queue node tells us "OutOfMemory" and then popping 
        them until all are popped.
    
    ***************************************************************************/
        
    private void fillPopCommand ( )
    {
        logger.info("Pushing items till full, popping till empty:");
        foreach (command; commands)
        {            
            logger.info("\t{} ...", command.name());
            
            try do command.push(this.epoll, this.queue_client, 5);
            while (command.info.status != QueueConst.Status.OutOfMemory)
            catch (UnexpectedResultException e) 
            {
                if (e.result != QueueConst.Status.OutOfMemory)
                {
                    throw e;
                }
            }
                        
            try do command.pop(this.epoll, this.queue_client, 2);            
            while (command.itemsLeft > 0)
            catch (Exception e) {}
                        
            if (this.barrier !is null) this.barrier.wait();
               
            auto remained = command.getChannelSize(this.epoll, this.queue_client); 
            if (remained != 0)
            {
                logger.error("Not all items where popped. {} items remained",
                             remained);
                
                throw new Exception("Not all items where popped!");
            }
            
            command.finish();
        }
    }
          
    /***************************************************************************
    
        Runs the fillPopCommand test which consists in pushing x entries
        until the queue node tells us "OutOfMemory" and then consuming 
        them until all are consumed
        
        TODO: currently no way of knowing when the consumer is done.
    
    ***************************************************************************/
        
    private void fillConsumeComand ( )
    {
        logger.info("Pushing items till full, consuming till empty:");
        foreach (command; commands)
        {            
            logger.info("\t{} ...", command.name());
            
            try do  command.push(this.epoll, this.queue_client, 10 );
            while (command.info.status != QueueConst.Status.OutOfMemory)
            catch (UnexpectedResultException e) 
            {
                if (e.result != QueueConst.Status.OutOfMemory)
                {
                    throw e;
                }
            }
            
            command.consume(this.epoll, this.queue_client);
            
            command.finish();
        }
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
    
    ICommand command;
    
    /***************************************************************************
    
        
    
    ***************************************************************************/
        
    this ( char[] config, ICommand command )
    {       
        this.epoll  = new EpollSelectDispatcher;
                
        this.queue_client = new QueueClient(epoll, 10);
        this.queue_client.addNodes(config);
        
        this.command = command;
        
        super(&this.run);
        
        this.logger = Log.lookup("ConsumerThread");
    }
    
    void run ( )
    {        
        logger.trace("running...");
        scope (exit) logger.trace("exiting ...");
        
        try this.command.consume(this.epoll, this.queue_client);
        catch (Exception e) logger.error("Consumer Exception: {}", e.msg);
    }
    
    public void stopConsume ( )
    {
        this.epoll.shutdown;
    }    
}
    
