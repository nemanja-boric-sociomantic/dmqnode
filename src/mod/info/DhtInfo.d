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
        The following flags can be passed to ne ised wotj monitor mode:
        -w = width of monitor display (number of columns)
        -M = show records and bytes as metric (K, M, G, T) in the monitor
            display

    Inherited from super class:
        -h = display help
        -x = number of connection to use when connecting to dht(s)

*******************************************************************************/



/*******************************************************************************

    Imports

*******************************************************************************/


module src.mod.info.DhtInfo;



private import tango.core.Thread;

private import tango.io.FilePath;



private import ocean.core.Array : appendCopy;

private import ocean.text.Arguments;

private import ocean.io.Stdout;



private import swarm.dht.DhtClient;



private import src.mod.model.DhtTool : MultiDhtTool;

private import src.mod.info.NodeInfo;


private import src.mod.info.modes.model.IMode;

private import src.mod.info.modes.HashRangesMode;

private import src.mod.info.modes.ApiVersionMode;

private import src.mod.info.modes.NumOfConnectionsMode;

private import src.mod.info.modes.MinimalMode;

private import src.mod.info.modes.GetContentsMode;

private import src.mod.info.modes.MonitorMode;



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

        Tracks the nodes that are associated with each Dht.

    ***************************************************************************/

    private DhtWrapper[] dht_wrappers;



    /***************************************************************************

        Main process method. Runs the tool based on the passed command line
        arguments.

    ***************************************************************************/

    protected void process_ ( )
    {
        assert(super.dht_nodes_config.length == super.dhts.length,
                "The proper number Of Dhts wasn't parsed correctly.");

        foreach (i, dht; super.dhts)
        {
            DhtWrapper wrapper;
            wrapper.dht = dht;

            auto dht_file_path = FilePath(super.dht_nodes_config[i]);
            wrapper.dht_id = dht_file_path.name;
            this.dht_wrappers ~= wrapper;



            if (wrapper.dht_id.length > longest_dht_name)
                longest_dht_name = wrapper.dht_id.length;

            foreach ( dht_node; dht.nodes )
            {
                auto node = NodeInfo(dht_node.address, dht_node.port,
                        dht_node.hash_range_queried, dht_node.min_hash,
                        dht_node.max_hash);

                this.dht_wrappers[$-1].nodes ~= node;

                auto name_len = node.nameLength(); //Re-check this line
                if ( name_len > longest_node_name )
                {
                    longest_node_name = name_len;
                }
            }

        }


        IMode[] display_modes;

        foreach (wrapper; this.dht_wrappers)
        {
            if ( this.connections )
            {

                display_modes ~= new NumOfConnectionsMode (wrapper,
                                                    &this.notifier);
            }
            else if (this.data)
            {
                display_modes ~= new GetContentsMode (wrapper, &this.notifier,
                                                        this.verbose);
            }
            else if (this.api_version)
            {
                display_modes ~= new ApiVersionMode (wrapper, &this.notifier);
            }
            else if (this.hash_ranges)
            {
                display_modes ~= new HashRangesMode (wrapper, &this.notifier);
            }
            else if (this.monitor)
            {
                display_modes ~= new MonitorMode(wrapper, &this.notifier,
                    this.monitor_num_columns, this.monitor_metric_display);
            }
            else if (this.minimal)
            {
                display_modes ~= new MinimalMode(wrapper, &this.notifier);
                this.longest_node_name = this.longest_dht_name;
            }
        }



        do
        {
            this.dht_errors.length = 0;
            bool again;
            do
            {
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

                 mode.display(longest_node_name);
            }

            this.displayErrors();

            Thread.sleep(periodic);
        }
        while (periodic)
    }


    /***************************************************************************

        Adds command line arguments specific to this tool.

        Params:
            args = command line arguments object to add to

    ***************************************************************************/

    override protected void addArgs_ ( Arguments args )
    {
        args("source").params(1, 42).required().aliased('S').help("paths of dhtnodes.xml files defining dhts and their nodes to query");
        args("data").aliased('d').help("display the quantity of data stored in each node and each channel");
        args("verbose").aliased('v').help("verbose output, displays info per channel per node, and per node per channel");
        args("conns").aliased('c').help("displays the number of connections being handled per node");
        args("api").aliased('a').help("displays the api version of the dht nodes");
        args("range").aliased('r').help("display the hash ranges of the dht nodes");
        args("width").params(1).aliased('w').defaults("4").help("width of monitor display (number of columns)");
        args("metric").aliased('M').help("show records and bytes as metric (K, M, G, T) in the monitor display");
        args("minimal").aliased('m').help("run the monitor in minimal display mode, to save screen space");
        args("periodic").params(1).aliased('p').defaults("0").help("timeout period before repeating a request. "
            "If parameter value is 0 or not passed then monitoring is performed only once");
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

        Overridden dht error callback. Stores the error message for display
        after processing.  The error messages are displayed all together at the
        end of processing so that the normal output is still readable.
        Though it will be displayed after each iteration if periodic is used,


        Params:
            e = dht client error info

    ***************************************************************************/

    override protected void notifier ( DhtClient.RequestNotification info )
    {
        if ( info.type == info.type.Finished && !info.succeeded )
        {
            super.dht_error = true;
            this.dht_errors.appendCopy(info.message);
        }
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

