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

private import ocean.text.util.DigitGrouping;

private import ocean.util.log.PeriodicTrace;

private import swarm.queue.QueueClient;

private import swarm.queue.QueueConst;

private import tango.io.Stdout,
               tango.core.Memory;



/*******************************************************************************

    Queue consumer class

*******************************************************************************/

public class QueueConsumer
{
    /***************************************************************************

        Epoll select dispatcher.

    ***************************************************************************/

    private EpollSelectDispatcher epoll;


    /***************************************************************************

        Queue client.

    ***************************************************************************/

    private QueueClient queue;


    /***************************************************************************

        Strings used for free / used memory formatting.
    
    ***************************************************************************/
    
    char[] free_str;
    
    char[] used_str;


    /***************************************************************************

        Parses and validates command line arguments.

        Params:
            args = arguments object
            arguments = command line args (excluding the file name)

        Returns:
            true if the arguments are valid

    ***************************************************************************/

    public bool parseArgs ( Arguments args, char[][] arguments )
    {
        args("source").required.params(1).aliased('S').help("config file listing queue nodes to connect to");
        args("channel").required.params(1).aliased('c').help("channel to consume");
        args("dump").aliased('d').help("dump consumed records to console");

        if ( !args.parse(arguments) )
        {
            Stderr.formatln("Invalid arguments:");
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

    public void run ( Arguments args )
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

              if ( value.length )
              {
                  num++;
              }

              size_t free, used;

              GC.usage(free, used);

              BitGrouping.format(free, this.free_str, "b");
              BitGrouping.format(used, this.used_str, "b");

              StaticPeriodicTrace.format("Memory used: {:d10}, free: {:d10}, consumed: {}", 
                                         this.used_str, this.free_str, num);
          }, &this.notifier);

        this.queue.assign(params);
        
        this.epoll.eventLoop;
    }


    /***************************************************************************

        Queue notification callback. As the only request invoked is a Consume
        request, which should never finish, this callback only fires with type
        Finished in the case of an error.
    
        Params:
            info = request notification info
    
    ***************************************************************************/
    
    private void notifier ( QueueClient.RequestNotification info )
    {
        if ( info.type == info.type.Finished )
        {
            Stderr.formatln("Queue: status={}, msg={}", info.status, info.message);
        }
    }
}

