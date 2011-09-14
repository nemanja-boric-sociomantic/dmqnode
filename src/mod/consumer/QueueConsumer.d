/*******************************************************************************

    Queue consumer

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        July 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.consumer.QueueConsumer;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.io.select.EpollSelectDispatcher;

private import ocean.text.Arguments;

private import ocean.util.log.PeriodicTrace;

private import swarm.queue.QueueClient;

private import swarm.queue.QueueConst;

private import tango.io.Stdout,
               tango.core.Memory;



/*******************************************************************************

    Queue consumer class

*******************************************************************************/

class QueueConsumer
{
    /***************************************************************************
    
        Singleton instance of this class, used in static methods.
    
    ***************************************************************************/

    private static typeof(this) singleton;

    static private typeof(this) instance ( )
    {
        if ( !singleton )
        {
            singleton = new typeof(this);
        }

        return singleton;
    }


    /***************************************************************************

        Parses and validates command line arguments.

        Params:
            args = arguments object
            arguments = command line args (excluding the file name)

        Returns:
            true if the arguments are valid

    ***************************************************************************/

    static public bool parseArgs ( Arguments args, char[][] arguments )
    {
        return instance().validateArgs(args, arguments);
    }


    /***************************************************************************
    
        Main run method, called by OceanException.run.
        
        Params:
            args = processed arguments
    
        Returns:
            always true
    
    ***************************************************************************/

    static public bool run ( Arguments args )
    {
        instance().process(args);
        return true;
    }


    /***************************************************************************

        Epoll select dispatcher.

    ***************************************************************************/

    private EpollSelectDispatcher epoll;


    /***************************************************************************

        Queue client.

    ***************************************************************************/

    private QueueClient queue;


    /***************************************************************************
    
        Validates command line arguments.
    
        Params:
            args = arguments processor
            arguments = command line args

        Returns:
            true if arguments are valid

    ***************************************************************************/

    private bool validateArgs ( Arguments args, char[][] arguments )
    {
        args("source").required.params(1).aliased('S').help("config file listing queue nodes to connect to");
        args("channel").required.params(1).aliased('c').help("channel to consume");
        args("dump").aliased('d').help("dump consumed records to console");

        if ( arguments.length && !args.parse(arguments) )
        {
            args.displayErrors();
            return false;
        }

        return true;
    }


    /***************************************************************************

        Initialises a queue client and connects to the nodes specified in the
        command line arguments. Gets information from all connected queue nodes
        and displays it in two tables.

        Params:
            args = processed arguments

    ***************************************************************************/

    private void process ( Arguments args )
    {
        uint pushed, returned;

        this.epoll = new EpollSelectDispatcher;

        this.queue = new QueueClient(this.epoll);

        this.queue.addNodes(args.getString("source"));

        Stdout.formatln("Consuming from channel '{}'", args.getString("channel"));

        size_t num;
        
        auto params = this.queue.consume(args.getString("channel"), 
                      ( QueueClient.RequestContext context, char[] value )
                      {
                          if ( args.getBool("dump") )
                          {
                              Stdout.formatln("'{}'", value);
                          }
                          
                          size_t free, used;
                          
                          GC.usage(free, used);
                          
                          StaticPeriodicTrace.format("Memory used: {:d10}, free: {:d10}, consumed: {}", 
                                              used/1024.0/1024.0, free/1024.0/1024.0, ++num);
                                         
                      }, &this.notifier);

        this.queue.assign(params);
        
        this.epoll.eventLoop;
    }

    void notifier ( QueueClient.RequestNotification info )
    {
        Stderr.formatln("Queue: status={}, msg={}", info.status, info.message);
    } 
    

}

