/*******************************************************************************

    Queue consumer

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        July 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.producer.QueueProducer;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.io.select.EpollSelectDispatcher;

private import ocean.text.Arguments;

private import ocean.util.log.PeriodicTrace;

private import swarm.queue.QueueClient;

private import swarm.queue.QueueConst;

private import tango.io.Stdout,
               tango.core.Memory,
               Integer = tango.text.convert.Integer;



/*******************************************************************************

    Queue consumer class

*******************************************************************************/

class QueueProducer
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
    
    Arguments args;

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

        this.args = args;
        
        this.queue.addNodes(args.getString("source"));

        Stdout.formatln("Consuming from channel '{}'", args.getString("channel"));

        this.startProduce;
        
        this.epoll.eventLoop;
        
        Stdout.formatln("..EXIT");
    }
    
    ulong num;
    char[512] buf;
                
    void startProduce ( )
    {

        auto params = this.queue.produce(args.getString("channel"), 
          &this.producer, &this.notifier);

        this.queue.assign(params);
        
    }
    
    void producer ( QueueClient.RequestContext context, QueueClient.IProducer producer )
    {
        char[] str = Integer.format(buf, num++);
      
        producer(str);
      
        if ( args.getBool("dump") )
        {
            Stdout.formatln("'{}'", str);
        }
      
        size_t free, used;
      
        GC.usage(free, used);
      
        StaticPeriodicTrace.format("Memory used: {:d10}, free: {:d10}, produced: {}", 
                                   used/1024.0/1024.0, free/1024.0/1024.0, num);
                       
    }
    

    /***************************************************************************

        Queue notification callback. As the only request invoked is a Produce
        request, which should never finish, this callback only fires with type
        Finished in the case of an error.
    
        Params:
            info = request notification info
    
    ***************************************************************************/
    
    void notifier ( QueueClient.RequestNotification info )
    {        
        if (info.type == info.type.Finished)
        {
        	Stderr.formatln("Queue: status={}, msg={}", info.status, info.message);
            startProduce();
        }
    } 
}

