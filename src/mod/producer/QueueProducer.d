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

private import ocean.text.util.DigitGrouping;

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

        Arguments read from the command line. Passed as a reference to the
        run() method.

    ***************************************************************************/

    private Arguments args;


    /***************************************************************************

        Counter incremented once per record sent.

    ***************************************************************************/

    private ulong num;


    /***************************************************************************

        Buffer used for writing records.

    ***************************************************************************/

    private char[] buf;


    /***************************************************************************

        Strings used for free / used memory formatting.

    ***************************************************************************/

    char[] free_str;

    char[] used_str;


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
        args("size").params(1).defaults("8").aliased('s').help("size (in bytes) of records to produce. If >= 8, then the first 8 bytes will contain the record number as a ulong");
        args("dump").aliased('d').help("dump consumed records to console");
        args("reconnect").aliased('r').help("reconnect on queue error");

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

        this.args = args;

        this.buf.length = this.args.getInt!(size_t)("size");

        this.queue.addNodes(this.args.getString("source"));

        Stdout.formatln("Producing to channel '{}', record size = {} bytes", this.args.getString("channel"), this.buf.length);

        this.startProduce;
        
        this.epoll.eventLoop;
        
        Stdout.formatln("...EXIT");
    }


    /***************************************************************************

        Assigns a produce request to the queue client.

    ***************************************************************************/

    private void startProduce ( )
    {
        auto params = this.queue.produce(args.getString("channel"), 
          &this.producer, &this.notifier);

        this.queue.assign(params);
    }


    /***************************************************************************

        Produce request callback. Writes the next record to the producer.

        Params:
            context = request context (not used)
            producer = interface to receive a value to send

    ***************************************************************************/

    private void producer ( QueueClient.RequestContext context, QueueClient.IProducer producer )
    {
        if ( this.buf.length >= 8 )
        {
            *(cast(ulong*)this.buf.ptr) = this.num++;
        }

        producer(this.buf);

        if ( this.args.getBool("dump") )
        {
            Stdout.formatln("'{}'", this.buf);
        }

        size_t free, used;
        GC.usage(free, used);

        BitGrouping.format(free, this.free_str, "b");
        BitGrouping.format(used, this.used_str, "b");

        StaticPeriodicTrace.format("Memory used: {:d10}, free: {:d10}, produced: {}", 
                                   this.used_str, this.free_str, num);
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

            if ( this.args.getBool("reconnect") )
            {
                this.startProduce;
            }
        }
    }
}

