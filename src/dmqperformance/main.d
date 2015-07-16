/*******************************************************************************

    DMQ performance tester

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        October 2011: Initial release

    authors:        Gavin Norman

    Repeatedly performs a series of one or more pushes, followed by an equal
    number of pops. The time taken per request and for the whole group of push /
    pop requests is measured.

    Command line args:
        -S = path of DMQ nodes file
        -c = the number of pushes / pops to perform sequentially before
             switching from pushing to popping or vice versa (default is 1000)
        -p = the number of parallel pushes / pops to perform (default is 1)
        -s = size of record to push / pop (in bytes, default is 1024)

*******************************************************************************/

module dmqperformance.main;


/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.io.select.EpollSelectDispatcher;

private import ocean.text.Arguments;

private import ocean.util.log.StaticTrace;

private import ocean.math.SlidingAverage;

private import ocean.util.app.VersionedCliApp;

private import Version;

private import swarm.dmq.DmqClient;

private import swarm.dmq.DmqConst;

private import tango.io.Stdout;

private import tango.time.StopWatch;


/*******************************************************************************

    Main

    Params:
        arguments = command line arguments

*******************************************************************************/

void main ( char[][] cl_args )
{
    auto app = new DmqPerformance;
    return app.main(cl_args);
}


/*******************************************************************************

    DMQ performance tester class

*******************************************************************************/

public class DmqPerformance : VersionedCliApp
{
    /***************************************************************************

        Epoll select dispatcher.

    ***************************************************************************/

    private EpollSelectDispatcher epoll;


    /***************************************************************************

        DMQ client.

    ***************************************************************************/

    private DmqClient dmq;


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

        Buffer for record being sent to the DMQ.

    ***************************************************************************/

    private char[] record;


    /***************************************************************************

        DMQ client message formatting buffer.

    ***************************************************************************/

    private char[] message_buffer;


    /***************************************************************************

        Constructor.

    ***************************************************************************/

    public this ( )
    {
        const char[] name = "DMQ performance tester";
        const char[] desc = "performing (-c) pushes then (-c) pops each cycle,"
                            " with up to (-p) requests in parallel";
        const char[] usage;
        const char[] help;

        super ( name, desc, Version, usage, help);
    }


    /***************************************************************************

        Set up the arguments parser for the app.

        Params:
            app = application instance
            argument = arguments

    ***************************************************************************/

    public void setupArgs( IApplication app, Arguments args )
    {
        args("source").required.params(1).aliased('S').
          help("config file listing DMQ nodes to connect to");
        args("count").aliased('c').params(1).defaults("1000").
          help("the number of pushes / pops to perform sequentially before"
          " switching from pushing to popping or vice versa (default is 1000)");
        args("parallel").aliased('p').params(1).defaults("1").
         help("the number of parallel pushes / pops to perform (default is 1)");
        args("size").aliased('s').params(1).defaults("1024").
          help("size of record to push / pop (in bytes, default is 1024)");
    }


    /***************************************************************************

        Should validate the commandline arguments here.

        Params:
            app = application instance
            argument = arguments

        Returns:
            null, which means the arguments are valid

    ***************************************************************************/

    public char[] validateArgs( IApplication app, Arguments args )
    {
        return null;
    }


    /***************************************************************************

        Process the arguments here

        Params:
            app = application instance
            argument = arguments

    ***************************************************************************/

    public void processArgs( IApplication app, Arguments args )
    {
    }


    /***************************************************************************

        Initialises a DMQ client and connects to the nodes specified in the
        command line arguments.

        Params:
            args = processed arguments

        Returns:
            0

    ***************************************************************************/

    public int run ( Arguments args )
    {
        auto count = args.getInt!(uint)("count");

        auto parallel = args.getInt!(uint)("parallel");

        this.record.length = args.getInt!(size_t)("size");

        this.epoll = new EpollSelectDispatcher;

        this.dmq = new DmqClient(this.epoll, parallel);

        this.dmq.addNodes(args.getString("source"));

        Stdout.formatln("DMQ performance tester:");
        Stdout.formatln("    performing {} pushes then {} pops each cycle, "
            "with up to {} requests in parallel", count, count, parallel);
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
                this.dmq.assign(this.dmq.push("test", &pushCallback, &notifier));
                flush();
            }
            flush(true);

            // Pops
            pushing = false;
            this.batch_timer.start;
            for ( uint i; i < count; i++ )
            {
                this.dmq.assign(this.dmq.pop("test", &popCallback, &notifier));
                flush();
            }
            flush(true);

            Stdout.formatln("");
        }

        return 0;
    }


    /***************************************************************************

        DMQ push callback.

        Params:
            context = request context (unused)

        Returns:
            record to push

    ***************************************************************************/

    private char[] pushCallback ( DmqClient.RequestContext context )
    {
        return this.record;
    }


    /***************************************************************************

        DMQ pop callback.

        Params:
            context = request context (unused)
            data = record popped

    ***************************************************************************/

    private void popCallback ( DmqClient.RequestContext context, char[] data )
    {
    }


    /***************************************************************************

        DMQ notification callback. Updates the timers with the time taken to
        complete this request.

        Params:
            info = request notification info

    ***************************************************************************/

    private void notifier ( DmqClient.RequestNotification info )
    {
        if ( info.type == info.type.Finished )
        {
            if ( info.succeeded )
            {
                auto Us = this.request_timer.microsec;

                switch ( info.command )
                {
                    case DmqConst.Command.E.Push:
                        this.pushes += Us;
                    break;

                    case DmqConst.Command.E.Pop:
                        this.pops += Us;
                    break;

                    default:
                        assert(false);
                }
            }
            else
            {
                Stderr.formatln("Error in DMQ request: {}",
                    info.message(this.message_buffer));
            }
        }
    }
}

