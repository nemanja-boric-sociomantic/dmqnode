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
               tango.io.Stdout,
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
    
        Name of the channel that we are testing
    
    ***************************************************************************/
        
    char[] channel;
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
    
        Constructor
        
        Params:
            args =     arguments to test
            commands = instances of command executers
    
    ***************************************************************************/
        
    this ( Arguments args, ICommand[] commands ) 
    {   
        this.epoll  = new EpollSelectDispatcher;
                
        this.queue_client = new QueueClient(epoll, 1);
        this.queue_client.addNodes(args("config").assigned[0]);     
        
        this.args = args;
        
        this.channel = "test_channel_" ~ 
                        Integer.toString(cast(size_t)cast(void*) commands);
        
        this.config = args("config").assigned[0];
        
        this.commands = commands;
        
        super(&this.run);
    }
        
    /***************************************************************************
    
        Runs the tests
    
    ***************************************************************************/
        
    public void run ()
    {
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
        
        Trace.formatln("{} Done", cast(size_t)cast(void*)this);
    }
      
    /***************************************************************************
    
        Runs the popperCommand test which consists in pushing x entries
        and popping them after each push command.
    
    ***************************************************************************/
        
    private void popperCommand ( )
    {
        Trace.formatln("Pushing and popping items:");
        foreach (command; commands)
        {            
            Trace.formatln("\t{} ...", command.name());
            
            for (size_t i = 0; i < 10_000; ++i)
            {
                command.push(this.epoll, this.queue_client, this.channel, 5);
                command.pop(this.epoll, this.queue_client, this.channel, 5);
            }
            
            Thread.sleep(1);
            
            if (!command.done)
            {
                Trace.formatln("Consumer did not consume all pushed items");
            }
        }
    }
          
    /***************************************************************************
    
        Runs the consumerCommand test which consists in pushing x entries
        and consuming them at the same time
    
    ***************************************************************************/
        
    private void consumerCommand ( )
    {
        Trace.formatln("Pushing and consuming items:");
        foreach (command; commands)
        {
            Trace.formatln("\t{} ...", command.name());
            
            auto consumer = new Consumer(this.config, this.channel, command);
            consumer.start;
            
            for (size_t i = 0; i < 100_000; ++i)
            {
                command.push(this.epoll, this.queue_client, this.channel, 2);    
            }
            
            Thread.sleep(1);
            
            if (!command.done)
            {
                Trace.formatln("Consumer did not consume all pushed items");
            }
            
            consumer.stopConsume;
            
            consumer.join;
        }
    }
          
    /***************************************************************************
    
        Runs the fillPopCommand test which consists in pushing x entries
        until the queue node tells us "OutOfMemory" and then popping 
        them until all are popped.
    
    ***************************************************************************/
        
    private void fillPopCommand ( )
    {
        Trace.formatln("Pushing items till full, popping till empty:");
        foreach (command; commands)
        {            
            Trace.formatln("\t{} ...", command.name());
            
            try do  command.push(this.epoll, this.queue_client, this.channel, 2 );
            while (command.info.status != QueueConst.Status.OutOfMemory)
            catch (Exception e)
            {
                if (command.info.status != QueueConst.Status.OutOfMemory)
                {
                    throw e;
                }
            }
            
            do command.pop(this.epoll, this.queue_client, this.channel, 2);            
            while (!command.done);
            
            Thread.sleep(1);
            
            if (!command.done)
            {
                Trace.formatln("Consumer did not consume all pushed items");
            }
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
        Trace.formatln("Pushing items till full, consuming till empty:");
        foreach (command; commands)
        {            
            Trace.formatln("\t{} ...", command.name());
            
            do  command.push(this.epoll, this.queue_client, this.channel, 10 );
            while (command.info.status != QueueConst.Status.OutOfMemory)
           
            auto consumer = new Consumer(this.config, this.channel, command);
            consumer.start;
            
            Thread.sleep(1);
            
            while ( !command.done )
            {
                Trace.formatln("Waiting for consumer to finish ... ");
                Thread.sleep(1);
            }
                        
            consumer.stopConsume;
            
            consumer.join;
        }
    }
}

class Consumer : Thread
{
    /***************************************************************************
    
        Name of the channel that we are writing into.
    
    ***************************************************************************/
        
    char[] channel;
    
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
        
    this ( char[] config, char[] channel, ICommand command )
    {       
        this.epoll  = new EpollSelectDispatcher;
                
        this.queue_client = new QueueClient(epoll, 10);
        this.queue_client.addNodes(config);     
        
        this.channel = channel;
        
        this.command = command;
        
        super(&this.run);
    }
    
    void run ( )
    {        
        this.command.consume(this.epoll, this.queue_client, this.channel);
    }
    
    public void stopConsume ( )
    {
        this.epoll.shutdown;
    }    
}
    
