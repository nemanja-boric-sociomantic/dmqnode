/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman, Hatem Oraby

    Display information about a dht - the names of the channels, and optionally
    the number of records & bytes per channel.
    In it's current version, no two modes can be used together (i.e. you can't
    specify two flag modes at the same run).


    Command line parameters:
        -S = dhtnodes.xml file for dht to query
        -c = display the number of connections being handled per node
        -p = run the mode perdiocally every x seconds where x is the value
            pass to this flag. If not provided, then it designated mode will run
            only once then quits.

        Modes:
        -d = display the quantity of data stored in each node and each channel
        -v = verbose output, displays info per channel per node, and per node
            per channel
        -a = display the api version of the dht nodes
        -r = display the hash ranges of the dht nodes
        -m = display in Dhts im minimal mode.

        If none of the modes flags is specified, them tje Monitor mode is used.
        The following flags can be passed to be used with the monitor mode:
        -w = width of monitor display (number of columns)
        -M = show records and bytes as metric (K, M, G, T) in the monitor
            display

    Inherited from super class:
        -h = display help
        -x = number of connection to use when connecting to dht(s)

*******************************************************************************/

module src.mod.info.DhtInfo;


/*******************************************************************************

    Imports

*******************************************************************************/


private import src.mod.model.DhtTool : MultiDhtTool;

private import src.mod.info.NodeInfo;


private import src.mod.info.modes.model.IMode;

private import src.mod.info.modes.HashRangesMode;

private import src.mod.info.modes.ApiVersionMode;

private import src.mod.info.modes.NumOfConnectionsMode;

private import src.mod.info.modes.MinimalMode;

private import src.mod.info.modes.GetContentsMode;

private import src.mod.info.modes.MonitorMode;


private import swarm.dht.DhtClient;


private import ocean.core.Array : appendCopy;

private import ocean.text.Arguments;

private import ocean.io.Stdout;

private import ocean.io.select.event.TimerEvent;


private import tango.core.Thread;

private import tango.io.FilePath;

private import tango.time.StopWatch;

private import Integer = tango.text.convert.Integer;


/*******************************************************************************

    The class parses the command line arguments, runs the event-loop and calls
    the appropriate mode.

*******************************************************************************/


class DhtInfo : MultiDhtTool
{
    /***************************************************************************

        Toggle monitor display (default if no other options are specified).

    ***************************************************************************/

    private bool monitor;

 
    /***************************************************************************
    
        Toggle whether or not the minimal display should be used.

    ***************************************************************************/

    private bool minimal;
 

    /***************************************************************************

        Number of columns for monitor display.

    ***************************************************************************/

    private size_t monitor_num_columns;


    /***************************************************************************

        Monitor metric / normal integer display toggle.

    ***************************************************************************/

    private bool monitor_metric_display;


    /***************************************************************************
    
        Toggle data output.

    ***************************************************************************/
    
    private bool data;


    /***************************************************************************
    
        Toggle verbose output.
    
    ***************************************************************************/

    private bool verbose;


    /***************************************************************************
    
        Toggle output of number of connections being handled per node.
    
    ***************************************************************************/
    
    private bool connections;


    /***************************************************************************

        Toggle output of the nodes' api version.

    ***************************************************************************/

    private bool api_version;


    /***************************************************************************

        Memorizes the longest node name for display purposes.

    ***************************************************************************/

    private size_t longest_node_name;


    /***************************************************************************

        Memorizes the longest DHT name for display purposes.

    ***************************************************************************/

    private size_t longest_dht_name;


    /***************************************************************************

        Toggle output of the nodes' hash ranges.
    
    ***************************************************************************/
    
    private bool hash_ranges;


    /***************************************************************************

        List of dht error messages which occurred during processing
    
    ***************************************************************************/

    private char[][] dht_errors;


    /***************************************************************************

        Stop watch to count time elapsed since we started querying.

    ***************************************************************************/

    private uint periodic;


    /***************************************************************************

        Kepps tracks of all the display modes instances.

    ***************************************************************************/

    private IMode[] display_modes;


    /***************************************************************************

        The thresholds (in seconds) after which the progressChecker will start
        reporting late nodes.

    ***************************************************************************/

    private uint error_threshold_secs;


    /***************************************************************************

        Measures how long did the nodes take to responds.

    ***************************************************************************/

    private StopWatch sw;


    /***************************************************************************

        The interval at which the progress_checker handler will be called.

    ***************************************************************************/

    private uint handler_interval_msecs = 100;


    /***************************************************************************

        Used for formatting purposes.

    ***************************************************************************/

    private bool mention_once;


    /***************************************************************************

        Main process method. The method creates a DHT wrappers and the
        appropriate display-modes. The method also contains the main event-loop.
        The event-loop consists of two phases:
        - The request phase where all the display-modes fire their asynchronous
        calls to the DHT.
        - The The display phase where all the disply-modes prints their results.

    ***************************************************************************/

    protected void process_ ( )
    {
        assert(super.dht_nodes_config.length == super.dhts.length,
                "The proper number Of Dhts wasn't parsed correctly.");

        foreach (i, dht; super.dhts)
        {
            auto dht_id = FilePath(super.dht_nodes_config[i]).name;
            IMode mode;

            if (dht_id.length > longest_dht_name)
                longest_dht_name = dht_id.length;

            if ( this.connections )
            {
                mode = new NumOfConnectionsMode (dht, dht_id,
                                                &this.errorCallback);
                this.display_modes ~= mode;

                if (mode.getLongestNodeName() > this.longest_node_name)
                    this.longest_node_name = mode.getLongestNodeName();
            }
            else if (this.data)
            {
                mode = new GetContentsMode (dht, dht_id, &this.errorCallback,
                                            this.verbose);
                this.display_modes ~= mode;

                if (mode.getLongestNodeName() > this.longest_node_name)
                    this.longest_node_name = mode.getLongestNodeName();
            }
            else if (this.api_version)
            {
                mode = new ApiVersionMode (dht, dht_id, &this.errorCallback);
                this.display_modes ~= mode;

                if (mode.getLongestNodeName() > this.longest_node_name)
                    this.longest_node_name = mode.getLongestNodeName();
            }
            else if (this.hash_ranges)
            {
                mode = new HashRangesMode (dht, dht_id, &this.errorCallback);
                this.display_modes ~= mode;

                if (mode.getLongestNodeName() > this.longest_node_name)
                    this.longest_node_name = mode.getLongestNodeName();
            }
            else if (this.monitor)
            {
                mode = new MonitorMode(dht, dht_id, &this.errorCallback,
                                        this.monitor_num_columns,
                                        this.monitor_metric_display);
                this.display_modes ~= mode;

                if (mode.getLongestNodeName() > this.longest_node_name)
                    this.longest_node_name = mode.getLongestNodeName();
            }
            else if (this.minimal)
            {
                mode = new MinimalMode(dht, dht_id, &this.errorCallback);
                this.display_modes ~= mode;

                if (mode.getDhtId().length > this.longest_node_name)
                    this.longest_node_name = mode.getLongestNodeName();
            }

        }

        auto progress_checker = new TimerEvent (&this.progressChecker);
        progress_checker.set(0, this.handler_interval_msecs,
                             0, this.handler_interval_msecs);

        do
        {
            this.dht_errors.length = 0;
            this.mention_once = false;

            bool again;
            do
            {

                super.epoll.register(progress_checker);
                sw =  StopWatch();
                sw.start();

                foreach (mode; display_modes)
                {
                    //Note: again holds the value of just the last run.
                    //In the current moment, we assume that display_modes
                    //contains instances of the same type, hence they will all
                    //return the same value from run in the same whole loop.
                    again = mode.run ();
                }
                super.epoll.eventLoop();
            }
            while ( again)

            foreach (mode; display_modes)
            {
                 mode.display(this.longest_node_name);
            }

            this.displayErrors();

            Thread.sleep(periodic);
        }
        while (periodic)
    }


    /***************************************************************************

        This method is called as a callback for the Timer. It should check
        for each DHT which nodes has finished and which hasn't yet.
        All the nodes that exceeds a certain threshold are reported.

        Return:
            Because this method is Timer handler, it should return a boolean
            whether it should run again after the previously set timeout (true)
            or shouldn't run again (false).
            The method will keep reporting that it want to run again until
            all the nodes has replied.

    ***************************************************************************/

    private bool progressChecker()
    {
        auto timeTaken = this.sw.microsec();
        if( timeTaken/(1000*1000) >= this.error_threshold_secs)
        {
            stdout.flush();

            bool[] empty;
            empty.length = this.display_modes.length;
            foreach (i, mode; this.display_modes)
            {
                auto remaining = mode.whoDidntFinish();
                if (remaining.length)
                {
                    if (!mention_once)
                    {
                        char[] header = "\aThe following is taking "
                                        "too long to respond:";
                        stdout.format(header);
                        stdout.newline();
                        mention_once = true;
                    }

                    auto secs = timeTaken / (1000*1000);
                    int msecs = (timeTaken/1000) % 1000;

                    //Many stdout.format calls to use various colors.
                    stdout.red_bg;
                    Stdout.format("Taking: {}.{:d3} secs",
                                    secs, msecs);

                    stdout.default_bg;
                    Stdout.format(" -- ");

                    Stdout.blue_bg;
                    Stdout.white;
                    stdout.format(mode.getDhtId());
                    stdout.default_colour;

                    stdout.default_bg;
                    Stdout.format(" -- ");

                    char[] line;
                    foreach (node; remaining)
                    {
                        Stdout.default_bg;
                        stdout.format(" ");

                        Stdout.red_bg;
                        stdout.format(node.address ~":" ~
                                Integer.toString(node.port));

                        Stdout.default_bg;
                        stdout.format("");
                    }

                    stdout.newline();
                }
                else
                {
                    empty[i] = true;
                }
            }

            foreach (entry; empty)
            {
                if (!entry)
                {
                    return true;
                }
            }

            return false;
        }

        return true;
    }

    
    /***************************************************************************

        Adds command line arguments specific to this tool.

        Params:
            args = command line arguments object to add to

    ***************************************************************************/

    override protected void addArgs_ ( Arguments args )
    {
        //Required parameters
        args("source").params(1, 42).required().aliased('S').help("paths of dhtnodes.xml files defining dhts and their nodes to query");
        //Display-modes
        args("data").aliased('d').help("display the quantity of data stored in each node and each channel")
            .conflicts("conns").conflicts("api").conflicts("range").conflicts("minimal").conflicts("width").conflicts("metric");

        args("verbose").aliased('v').help("verbose output, displays info per channel per node, and per node per channel")
            .requires("data");

        args("conns").aliased('c').help("displays the number of connections being handled per node")
            .conflicts("data").conflicts("api").conflicts("range").conflicts("minimal").conflicts("width").conflicts("metric");;

        args("api").aliased('a').help("displays the api version of the dht nodes")
            .conflicts("data").conflicts("conns").conflicts("range").conflicts("minimal").conflicts("width").conflicts("metric");

        args("range").aliased('r').help("display the hash ranges of the dht nodes")
            .conflicts("data").conflicts("conns").conflicts("api").conflicts("minimal").conflicts("width").conflicts("metric");

        args("minimal").aliased('m').help("run the monitor in minimal display mode, to save screen space")
            .conflicts("data").conflicts("conns").conflicts("api").conflicts("range").conflicts("width").conflicts("metric");

        //Monitor mode optional parameters
        args("width").params(1).aliased('w').defaults("4").help("width of monitor display (number of columns)")
            .conflicts("data").conflicts("conns").conflicts("api").conflicts("range").conflicts("minimal");

        args("metric").aliased('M').help("show records and bytes as metric (K, M, G, T) in the monitor display")
            .conflicts("data").conflicts("conns").conflicts("api").conflicts("range").conflicts("minimal");

        //Optional Glabal Parameters
        args("periodic").params(1).aliased('p').defaults("0").help("timeout period before repeating a request. "
            "If parameter value is 0 or not passed then monitoring is performed only once");
        args("interval").params(1).aliased('i').defaults("1").help("The interval (in secs) aferwhich the "
           "application will report that one of the node didn't answered yet.");
    }


    /***************************************************************************

        Performs any additional command line argument validation which cannot be
        performed by the Arguments class.

        Params:
            args = command line arguments object to validate

        Returns:
            true if args are valid

    ***************************************************************************/

    override protected bool validArgs ( Arguments args )
    {
        if ( args.getInt!(size_t)("width") < 1 )
        {
            Stderr.formatln("Cannot display monitor with < 1 columns!");
            return false;
        }

        if ( args.getInt!(size_t)("periodic") < 0 )
        {
            Stderr.formatln("Cannot have periodic value param < 0");
            return false;
        }

        if ( args.getInt!(size_t)("interval") <= 0 )
        {
            Stderr.formatln("Cannot have checking intervals <= 0");
            return false;
        }

        return true;
    }


    /***************************************************************************

        Initialises this instance from the specified command line args.

        Params:
            args = command line arguments object to read settings from

    ***************************************************************************/

    protected void readArgs_ ( Arguments args )
    {
        char[] sources = args.getString("source");

        //super.dht_nodes_config = ;
        foreach ( ini; args("source").assigned )
        {
           super.dht_nodes_config ~= ini;
        }

        this.data = args.getBool("data");

        this.verbose = args.getBool("verbose");
        if ( this.verbose )
        {
            this.data = true;
        }

        this.connections = args.getBool("conns");

        this.api_version = args.getBool("api");

        this.hash_ranges = args.getBool("range");

        this.minimal = args.getBool("minimal");

        this.periodic = args.getInt!(int)("periodic");

        this.error_threshold_secs = args.getInt!(int)("interval");


        if ( !this.data && !this.verbose && !this.connections && !
            this.api_version && !this.hash_ranges && !this.minimal )
        {

            this.monitor = true;
            this.monitor_num_columns = args.getInt!(size_t)("width");
            this.monitor_metric_display = args.getBool("metric");
        }


    }


    /***************************************************************************

        Returns:
            false to indicate that the tool should not fail if any errors occur
            during node handshake

    ***************************************************************************/

    override protected bool strictHandshake ( )
    {
        return false;
    }


    /***************************************************************************

        This method is passed as a delegate to the display-modes, whenever
        one of the display-modes have an error that it want that it wants to
        announce it to the user, it will call this delegate method and pass
        the error to it.

        Params:
            msg = error to report

    ***************************************************************************/

    private void errorCallback (char[] msg)
    {
        super.dht_error = true;
        this.dht_errors.appendCopy(msg);
    }


    /***************************************************************************

        Displays any error messages which occurred during processing. The error
        messages are displayed all together at the end of processing so that
        the normal output is still readable.

    ***************************************************************************/

    private void displayErrors ( )
    {
        if ( this.dht_errors.length && !this.monitor )
        {
            Stderr.formatln("\nDht errors which occurred during operation:");
            Stderr.formatln("------------------------------------------------------------------------------");

            foreach ( i, err; this.dht_errors )
            {
                Stderr.formatln("  {,3}: {}", i, err);
            }
        }
    }
}

