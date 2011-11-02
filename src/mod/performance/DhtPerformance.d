/*******************************************************************************

    Dht performance tester

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        November 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.performance.DhtPerformance;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.core.ArrayMap;

private import ocean.io.select.EpollSelectDispatcher;

private import ocean.text.Arguments;

private import ocean.util.log.StaticTrace;

private import ocean.math.SlidingAverage;
private import ocean.math.Distribution;

private import swarm.dht.DhtClient;

private import swarm.dht.DhtConst;

private import tango.io.Stdout;

private import tango.time.StopWatch;



/*******************************************************************************

    Dht performance tester class

*******************************************************************************/

public class DhtPerformance
{
    /***************************************************************************

        Epoll select dispatcher.

    ***************************************************************************/

    private EpollSelectDispatcher epoll;


    /***************************************************************************

        Dht client.

    ***************************************************************************/

    private DhtClient dht;


    /***************************************************************************

        Time distribution tracker.

    ***************************************************************************/

    private Distribution!(ulong) requests;


    /***************************************************************************

        Average times measurer.

    ***************************************************************************/

    private SlidingAverage!(ulong) avg_times;


    /***************************************************************************

        Stopwatches to time the individual requests and the request batches.

    ***************************************************************************/

    private StopWatch request_timer;

    private StopWatch batch_timer;


    /***************************************************************************

        Buffer for record being sent to the dht.

    ***************************************************************************/

    private char[] record;

    
    /***************************************************************************

        Number of requests to perform per test cycle.

    ***************************************************************************/

    private uint count;


    /***************************************************************************

        String of request to perform.

    ***************************************************************************/

    private char[] command;
    

    /***************************************************************************

        Channel to perform requests over.

    ***************************************************************************/

    private char[] channel;
    

    /***************************************************************************

        Flag to dis/enable display of the number of requests which exceeded the
        specified time (this.timeout).

    ***************************************************************************/

    private bool show_timeouts;


    /***************************************************************************

        Microsecond limit, used for counting the number of requests which
        exceeded a certain time.

    ***************************************************************************/

    private ulong timeout;


    /***************************************************************************

        Stores the total time taken so far for a whole iteration.

    ***************************************************************************/

    private double total_time;


    /***************************************************************************

        The number of requests to perform in parallel.

    ***************************************************************************/

    private uint parallel;


    /***************************************************************************

        Counter used to track how many requests have been sent in parallel.

    ***************************************************************************/

    private uint parallel_count;


    /***************************************************************************

        Total number of iterations to perform (0 = infinite).

    ***************************************************************************/

    private uint iterations;


    /***************************************************************************

        Number of the current iteration.

    ***************************************************************************/

    private uint iteration_count;


    /***************************************************************************

        Parses and validates command line arguments. If the arguments are
        invalid, a help text is output.

        Params:
            args = arguments object
            arguments = command line args (excluding the file name)

        Returns:
            true if the arguments are valid

    ***************************************************************************/

    public bool parseArgs ( Arguments args, char[][] arguments )
    {
        args("source")    .aliased('S').params(1).required        .help("xml file listing dht nodes to connect to");
        args("channel")   .aliased('c').params(1).defaults("test").help("dht channel to operate on");
        args("number")    .aliased('n').params(1).defaults("1000").help("the number of requests to perform in each test cycle (default is 1000)");
        args("parallel")  .aliased('p').params(1).defaults("1")   .help("the number of parallel requests to perform (default is 1)");
        args("size")      .aliased('s').params(1).defaults("1024").help("size of record to put (in bytes, default is 1024)");
        args("command")   .aliased('m').params(1).defaults("put") .help("command to test (get / put, default is put)");
        args("timeout")   .aliased('t').params(1)                 .help("displays a count of the number of requests which took longer than the specified time (in μs)");
        args("iterations").aliased('i').params(1).defaults("0")   .help("number of test cycles to perform (default is 0, infinite)");

        if ( !args.parse(arguments) )
        {
            Stderr.formatln("Invalid arguments:");
            args.displayErrors();
            return false;
        }

        return true;
    }


    /***************************************************************************

        Performs the performance test indicated by the command line arguments.

        Params:
            args = processed arguments

    ***************************************************************************/

    public void run ( Arguments args )
    {
        // Read command line args
        this.readArgs(args);

        // Init epoll, dht client and times counter
        this.epoll = new EpollSelectDispatcher;

        this.dht = new DhtClient(this.epoll, parallel);
        if ( !this.initDht(args.getString("source")) )
        {
            Stderr.formatln("Node handshake failed");
            return;
        }

        this.requests = new Distribution!(ulong);
        this.avg_times = new SlidingAverage!(ulong)(1_000);

        // Startup message
        Stdout.formatln("Dht performance tester:");
        Stdout.formatln("    performing {} {} requests to channel '{}' each test cycle, with up to {} requets in parallel",
                count, command, channel, parallel);
        if ( command == "put" ) Stdout.formatln("    putting records of {} bytes", this.record.length);

        // Test cycle
        do
        {
            this.performRequests();

            this.display();

            this.iteration_count++;
        }
        while ( this.iterations == 0 || this.iteration_count < this.iterations);
    }


    /***************************************************************************

        Reads command line arguments.

        Params:
            args = processed arguments

    ***************************************************************************/

    private void readArgs ( Arguments args )
    {
        this.iterations = args.getInt!(uint)("iterations");

        this.count = args.getInt!(uint)("number");

        this.channel = args.getString("channel");

        this.command = args.getString("command");

        this.parallel = args.getInt!(uint)("parallel");

        this.show_timeouts = args.exists("timeout");
        this.timeout = args.getInt!(ulong)("timeout");

        this.record.length = args.getInt!(size_t)("size");
    }


    /***************************************************************************

        Attempts to connect to the dht.

        Params:
            nodes_xml = path of dhtnodes.xml file

        Returns:
            true if the dht handshake succeeded

    ***************************************************************************/

    private bool initDht ( char[] nodes_xml )
    {
        this.dht.addNodes(nodes_xml);

        bool handshake_ok;
        void handshakeCallback ( DhtClient.RequestContext c, bool ok )
        {
            handshake_ok = ok;
        }

        void handshakeNotifier ( DhtClient.RequestNotification info )
        {
            if ( info.type == info.type.Finished )
            {
                if ( !info.succeeded )
                {
                    Stderr.formatln("Error in dht handshake request: {}", info.message);
                }
            }
        }

        this.dht.nodeHandshake(&handshakeCallback, &handshakeNotifier);
        this.epoll.eventLoop;

        return handshake_ok;
    }


    /***************************************************************************

        Performs and times a batch of requests (one iteration).

    ***************************************************************************/

    private void performRequests ( )
    {
        this.total_time = 0;
        this.parallel_count = 0;

        this.batch_timer.start;

        for ( uint i; i < count; i++ )
        {
            this.assignRequest(i);
            this.flush();
        }
        this.flush(true);
    }


    /***************************************************************************

        Assigns a single request to the dht.

        Params:
            key = record key

    ***************************************************************************/

    private void assignRequest ( hash_t key )
    {
        switch ( this.command )
        {
            case "put":
                this.dht.assign(this.dht.put(this.channel, key, &this.putCallback, &this.notifier));
            break;

            case "get":
                this.dht.assign(this.dht.get(this.channel, key, &this.getCallback, &this.notifier));
            break;
        }
    }


    /***************************************************************************

        Checks whether the number of assigned dht requests is equal to the
        number of parallel requests specified on the command line, and calls the
        epoll event loop when this amount is reached, causing the requests to be
        performed.

        When the event loop exits, the console time display is updated.

        Params:
            force = if true, always flush, even if the number of parallel
                requests has not been reached (used at the end of a cycle to
                ensure that all requests have been processed)

    ***************************************************************************/

    private void flush ( bool force = false )
    {
        if ( force || ++this.parallel_count == this.parallel )
        {
            this.parallel_count = 0;
            this.request_timer.start;
            this.epoll.eventLoop;

            auto total_s = cast(float)this.batch_timer.microsec / 1_000_000.0;
            this.total_time = total_s;

            StaticTrace.format("avg: {}μs, count: {}, total: {}s",
                this.avg_times.average, this.requests.count, this.total_time).flush;
        }
    }


    /***************************************************************************

        Called at the end of an iteration. Displays time distribution info about
        the requests which were performed.

    ***************************************************************************/

    private void display ( )
    {
        const percentages = [0.5, 0.66, 0.75, 0.8, 0.9, 0.95, 0.98, 0.99, 0.995, 0.999, 1];

        Stdout.formatln("");

        if ( this.iterations == 0 )
        {
            Stdout.formatln("Iteration {} of infinite. Time distribution of {} {} requests:",
                    this.iteration_count + 1, this.count, this.command);
        }
        else
        {
            Stdout.formatln("Iteration {} of {}. Time distribution of {} {} requests:",
                    this.iteration_count + 1, this.iterations, this.count, this.command);
        }

        foreach ( i, percentage; percentages )
        {
            auto time = this.requests.percentValue(percentage);

            Stdout.formatln("{,5:1}% <= {}μs{}",
                    percentage * 100, time,
                    (i == percentages.length - 1) ? " (longest request)" : "");
        }

        if ( this.show_timeouts )
        {
            auto timed_out = this.requests.greaterThanCount(this.timeout);

            Stdout.formatln("\n{} requests ({,3:1}%) took longer than {}μs",
                    timed_out,
                    (cast(float)timed_out / cast(float)this.requests.length) * 100.0,
                    this.timeout);
        }

        this.requests.clear;

        Stdout.formatln("");
    }


    /***************************************************************************

        Dht put callback.

        Params:
            context = request context (unused)

        Returns:
            record to put
    
    ***************************************************************************/

    private char[] putCallback ( DhtClient.RequestContext context )
    {
        return this.record;
    }


    /***************************************************************************

        Dht getcallback.

        Params:
            context = request context (unused)
            data = record got

    ***************************************************************************/

    private void getCallback ( DhtClient.RequestContext context, char[] data )
    {
    }


    /***************************************************************************

        Dht notification callback. Updates the timers with the time taken to
        complete this request.

        Params:
            info = request notification info
    
    ***************************************************************************/

    private void notifier ( DhtClient.RequestNotification info )
    {
        if ( info.type == info.type.Finished )
        {
            if ( info.succeeded )
            {
                auto Us = this.request_timer.microsec;

                this.requests ~= Us;
                this.avg_times.push(Us);
            }
            else
            {
                Stderr.formatln("Error in dht request: {}", info.message);
            }
        }
    }
}

