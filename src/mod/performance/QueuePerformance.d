/*******************************************************************************

    Queue performance tester

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        October2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.performance.QueuePerformance;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.io.select.EpollSelectDispatcher;

private import ocean.text.Arguments;

private import ocean.util.log.StaticTrace;

private import ocean.math.SlidingAverage;

private import swarm.queue.QueueClient;

private import swarm.queue.QueueConst;

private import tango.io.Stdout;

private import tango.time.StopWatch;



/*******************************************************************************

    Queue performance tester class

*******************************************************************************/

public class QueuePerformance
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

        Class to measure the average time taken for a series of operations.

    ***************************************************************************/

    private class TimeDistribution
    {
        public ulong count;

        private SlidingAverage!(ulong) times;

        public this ( )
        {
            this.times = new SlidingAverage!(ulong)(1_000);
        }

        public void opAddAssign ( ulong time )
        {
            this.times.push(time);
            this.count++;
        }

        public double avg ( )
        {
            return this.times.average;
        }
    }


    private TimeDistribution pushes;

    private TimeDistribution pops;


    /***************************************************************************

        Stopwatches to time the individual requests and the request batches.

    ***************************************************************************/

    private StopWatch request_timer;

    private StopWatch batch_timer;


    /***************************************************************************

        Buffer for record being sent to the queue.

    ***************************************************************************/

    private char[] record;


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
        args("count").aliased('c').params(1).defaults("1000").help("the number of pushes / pops to perform sequentially before switching from pushing to popping or vice versa (default is 1000)");
        args("parallel").aliased('p').params(1).defaults("1").help("the number of parallel pushes / pops to perform (default is 1)");
        args("size").aliased('s').params(1).defaults("1024").help("size of record to push / pop (in bytes, default is 1024)");

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
        command line arguments.

        Params:
            args = processed arguments

    ***************************************************************************/

    public void run ( Arguments args )
    {
        auto count = args.getInt!(uint)("count");

        auto parallel = args.getInt!(uint)("parallel");

        this.record.length = args.getInt!(size_t)("size");

        this.epoll = new EpollSelectDispatcher;

        this.queue = new QueueClient(this.epoll, parallel);

        this.queue.addNodes(args.getString("source"));

        Stdout.formatln("Queue performance tester:");
        Stdout.formatln("    performing {} pushes then {} pops each cycle, with up to {} requets in parallel", count, count, parallel);
        Stdout.formatln("    pushing records of {} bytes", this.record.length);

        this.pushes = new TimeDistribution;
        this.pops = new TimeDistribution;

        while ( true )
        {
            double total_push_time, total_pop_time;
    
            bool pushing;
            uint parallel_count;

            // Function to flush requests and update time dispaly
            void flush ( bool force = false )
            {
                if ( force || ++parallel_count == parallel )
                {
                    parallel_count = 0;
                    this.request_timer.start;
                    this.epoll.eventLoop;
    
                    auto total_s = cast(float)this.batch_timer.microsec / 1_000_000.0;
                    if ( pushing ) total_push_time = total_s;
                    else           total_pop_time  = total_s;

                    StaticTrace.format("push: {}μs ({} = {}s), pop: {}μs ({} = {}s)",
                        pushes.avg, pushes.count, total_push_time,
                        pops.avg, pops.count, total_pop_time);
                }
            }

            // Pushes
            pushing = true;
            this.batch_timer.start;
            for ( uint i; i < count; i++ )
            {
                this.queue.assign(this.queue.push("test", &pushCallback, &notifier));
                flush();
            }
            flush(true);

            // Pops
            pushing = false;
            this.batch_timer.start;
            for ( uint i; i < count; i++ )
            {
                this.queue.assign(this.queue.pop("test", &popCallback, &notifier));
                flush();
            }
            flush(true);
    
            Stdout.formatln("");
        }
    }


    /***************************************************************************

        Queue push callback.

        Params:
            context = request context (unused)

        Returns:
            record to push
    
    ***************************************************************************/

    private char[] pushCallback ( QueueClient.RequestContext context )
    {
        return this.record;
    }


    /***************************************************************************

        Queue pop callback.

        Params:
            context = request context (unused)
            data = record popped

    ***************************************************************************/

    private void popCallback ( QueueClient.RequestContext context, char[] data )
    {
    }


    /***************************************************************************

        Queue notification callback. Updates the timers with the time taken to
        complete this request.

        Params:
            info = request notification info
    
    ***************************************************************************/

    private void notifier ( QueueClient.RequestNotification info )
    {
        if ( info.type == info.type.Finished )
        {
            if ( info.succeeded )
            {
                auto Us = this.request_timer.microsec;
    
                switch ( info.command )
                {
                    case QueueConst.Command.Push:
                        this.pushes += Us;
                    break;
    
                    case QueueConst.Command.Pop:
                        this.pops += Us;
                    break;
    
                    default:
                        assert(false);
                }
            }
            else
            {
                Stderr.formatln("Error in queue request: {}", info.message);
            }
        }
    }
}

