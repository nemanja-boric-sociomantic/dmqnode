/*******************************************************************************

    DHT node info

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman

    Display information about a dht - the names of the channels, and optionally
    the number of records & bytes per channel.

    Command line parameters:
        -S = dhtnodes.xml file for dht to query
        -d = display the quantity of data stored in each node and each channel
        -v = verbose output, displays info per channel per node, and per node
            per channel
        -c = display the number of connections being handled per node
        -a = display the api version of the dht nodes
        -r = display the hash ranges of the dht nodes
        -w = width of monitor display (number of columns)
        -m = show records and bytes as metric (K, M, G, T) in the monitor display

    Inherited from super class:
        -h = display help

*******************************************************************************/

module src.mod.info.DhtInfo;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.model.DhtTool;

private import src.mod.info.NodeInfo,
               src.mod.info.DhtMonitor;

private import swarm.dht.DhtClient,
               swarm.dht.DhtHash,
               swarm.dht.DhtConst;

private import ocean.core.Array;

private import ocean.text.Arguments;

private import ocean.text.util.DigitGrouping;

private import tango.core.Array;

private import tango.io.Stdout;

private import Integer = tango.text.convert.Integer;

private import tango.text.convert.Layout;



/*******************************************************************************

    Dht info tool

*******************************************************************************/

class DhtInfo : DhtTool
{
    /***************************************************************************
    
        Singleton parseArgs() and run() methods.
    
    ***************************************************************************/
    
    mixin SingletonMethods;


    /***************************************************************************

        Array of info on all dht nodes

    ***************************************************************************/

    private NodeInfo[] nodes;


    /***************************************************************************
    
        Toggle monitor display (default if no other options are specified).

    ***************************************************************************/

    private bool monitor;


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

        Toggle output of the nodes' hash ranges.
    
    ***************************************************************************/
    
    private bool hash_ranges;


    /***************************************************************************

        List of dht error messages which occurred during processing
    
    ***************************************************************************/

    private char[][] dht_errors;


    /***************************************************************************

        Overridden dht error callback. Stores the error message for display
        after processing.  The error messages are displayed all together at the
        end of processing so that the normal output is still readable.

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

        Main process method. Runs the tool based on the passed command line
        arguments.
    
        Params:
            dht = dht client to use
    
    ***************************************************************************/
    
    protected void process_ ( )
    {
        // Get node addresses/ports
        size_t longest_node_name;

        foreach ( node; super.dht.nodeRegistry )
        {
            this.nodes ~= NodeInfo(node.address, node.port,
                    node.hash_range_queried, node.min_hash, node.max_hash);

            auto name_len = this.nodes[$-1].nameLength();
            if ( name_len > longest_node_name )
            {
                longest_node_name = name_len;
            }
        }

        if ( monitor )
        {
            this.displayMonitor(longest_node_name);
        }

        // Display various forms of output
        if ( this.data )
        {
            this.displayContents(longest_node_name);
        }

        if ( this.connections )
        {
            this.displayNumConnections(longest_node_name);
        }

        if ( this.api_version )
        {
            this.displayApiVersions(longest_node_name);
        }

        if ( this.hash_ranges )
        {
            this.displayHashRanges(longest_node_name);
        }

        // Show any errors which occurred
        this.displayErrors();
    }


    /***************************************************************************
    
        Adds command line arguments specific to this tool.
        
        Params:
            args = command line arguments object to add to
    
    ***************************************************************************/

    override protected void addArgs_ ( Arguments args )
    {
        args("source").params(1).required().aliased('S').help("path of dhtnodes.xml file defining nodes to query");
        args("data").aliased('d').help("display the quantity of data stored in each node and each channel");
        args("verbose").aliased('v').help("verbose output, displays info per channel per node, and per node per channel");
        args("connections").aliased('c').help("displays the number of connections being handled per node");
        args("api").aliased('a').help("displays the api version of the dht nodes");
        args("range").aliased('r').help("display the hash ranges of the dht nodes");
        args("width").params(1).aliased('w').defaults("4").help("width of monitor display (number of columns)");
        args("metric").aliased('m').help("show records and bytes as metric (K, M, G, T) in the monitor display");
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

        return true;
    }


    /***************************************************************************
    
        Initialises this instance from the specified command line args.
    
        Params:
            args = command line arguments object to read settings from
    
    ***************************************************************************/
    
    protected void readArgs_ ( Arguments args )
    {
        super.dht_nodes_config = args.getString("source");

        this.data = args.getBool("data");

        this.verbose = args.getBool("verbose");
        if ( this.verbose )
        {
            this.data = true;
        }

        this.connections = args.getBool("connections");

        this.api_version = args.getBool("api");

        this.hash_ranges = args.getBool("range");

        if ( !this.data && !this.verbose && !this.connections && !this.api_version && !this.hash_ranges )
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

        Displays the hash range of each node.
    
        Params:
            dht = dht client to perform query with
            longest_node_name = the length of the longest node name string
    
    ***************************************************************************/
    
    private void displayHashRanges ( size_t longest_node_name )
    {
        Stdout.formatln("\nHash ranges:");
        Stdout.formatln("------------------------------------------------------------------------------");

        foreach ( i, node; this.nodes )
        {
            char[] name_str;
            node.name(name_str);
            this.outputHashRangeRow(i, name_str, longest_node_name, node.range_queried, node.min_hash, node.max_hash);
        }
    }


    /***************************************************************************

        Queries and displays the api version of each node.

        Params:
            dht = dht client to perform query with
            longest_node_name = the length of the longest node name string

    ***************************************************************************/
    
    private void displayApiVersions ( size_t longest_node_name )
    {
        Stdout.formatln("\nApi version:");
        Stdout.formatln("------------------------------------------------------------------------------");

        bool output;
        super.dht.assign(super.dht.getVersion(
                ( DhtClient.RequestContext context, char[] address, ushort port, char[] api_version )
                {
                    if ( api_version.length && !output )
                    {
                        Stdout.formatln("  {}:{} API: {}", address, port, api_version);
                        output = true;
                    }
                }, &this.notifier));
        super.epoll.eventLoop;
    }


    /***************************************************************************

        Queries and displays the number of connections being handled per node.
    
        Params:
            dht = dht client to perform query with
            longest_node_name = the length of the longest node name string

    ***************************************************************************/

    private void displayNumConnections ( size_t longest_node_name )
    {
        Stdout.formatln("\nConnections being handled:");
        Stdout.formatln("------------------------------------------------------------------------------");

        // Set the number of connections for all nodes to an invalid value
        foreach ( ref node; this.nodes )
        {
            node.connections = size_t.max;
        }

        // Query all nodes for their active connections
        super.dht.assign(super.dht.getNumConnections(
                ( DhtClient.RequestContext context, char[] node_address, ushort node_port, size_t num_connections )
                {
                    auto node = this.findNode(node_address, node_port);
                    assert(node, typeof(this).stringof ~ "Node mismatch!");

                    node.connections = num_connections;
                }, &this.notifier));
        super.epoll.eventLoop;

        // Display connections per node
        foreach ( i, node; this.nodes )
        {
            char[] node_name;
            node.name(node_name);
            bool node_queried = node.connections < size_t.max;
            this.outputConnectionsRow(i, node_name, longest_node_name, node_queried, node.connections - 1);
        }
    }


    /***************************************************************************

        Displays a nicely formatted monitor showing records & bytes per channel
        per node, along with node hash ranges and total record & bytes per node.

        Params:
            dht = dht client to perform query with
            longest_node_name = the length of the longest node name string

    ***************************************************************************/

    private void displayMonitor ( size_t longest_node_name )
    {
        // Get channel names
        size_t longest_channel_name;
        char[][] channel_names;

        this.getChannelNames(channel_names, longest_channel_name);

        // Get channel size info
        foreach ( channel; channel_names )
        {
            this.getChannelSize(channel);
        }

        DhtMonitor.display(this.nodes, this.monitor_num_columns, channel_names, this.monitor_metric_display);
    }


    /***************************************************************************

        Queries and displays the size of the contents of each channel and node.
    
        Params:
            dht = dht client to perform query with
            longest_node_name = the length of the longest node name string

    ***************************************************************************/

    private void displayContents ( size_t longest_node_name )
    {
        // Get channel names
        size_t longest_channel_name;
        char[][] channel_names;

        this.getChannelNames(channel_names, longest_channel_name);

        // Get channel size info
        foreach ( channel; channel_names )
        {
            this.getChannelSize(channel);
        }

        // Display channels
        Stdout.formatln("\nChannels:");
        Stdout.formatln("------------------------------------------------------------------------------");

        if ( this.verbose )
        {
            foreach ( i, channel; channel_names )
            {
                Stdout.formatln("Channel {}: {}:", i, channel);

                ulong channel_records, channel_bytes;
                foreach ( j, node; this.nodes )
                {
                    ulong records, bytes;
                    bool node_queried;
                    node.getChannelSize(channel, records, bytes, node_queried);
                    channel_records += records;
                    channel_bytes += bytes;

                    char[] node_name;
                    node.name(node_name);

                    this.outputSizeRow(j, node_name, longest_node_name, node_queried, records, bytes);
                }

                this.outputSizeTotal(longest_node_name, channel_records, channel_bytes);
            }
        }
        else
        {
            foreach ( i, channel; channel_names )
            {
                ulong records, bytes;
                foreach ( node; this.nodes )
                {
                    ulong channel_records, channel_bytes;
                    bool node_queried;
                    node.getChannelSize(channel, channel_records, channel_bytes, node_queried);
                    records += channel_records;
                    bytes += channel_bytes;
                }
    
                this.outputSizeRow(i, channel, longest_channel_name, true, records, bytes);
            }
        }

        // Display nodes
        Stdout.formatln("\nNodes:");
        Stdout.formatln("------------------------------------------------------------------------------");

        if ( this.verbose )
        {
            foreach ( i, node; this.nodes )
            {
                char[] node_name;
                node.name(node_name);
                Stdout.formatln("Node {}: {}:", i, node_name);

                ulong node_records, node_bytes;
                auto node_queried = node.channels.length > 0;

                if ( node_queried )
                {
                    foreach ( j, ch; node.channels )
                    {
                        this.outputSizeRow(j, ch.name, longest_channel_name, node_queried, ch.records, ch.bytes);
                        node_records += ch.records;
                        node_bytes += ch.bytes;
                    }
                }
                else
                {
                    this.outputSizeRow(0, "", longest_channel_name, node_queried, 0, 0);
                }

                this.outputSizeTotal(longest_channel_name, node_records, node_bytes);
            }
        }
        else
        {
            foreach ( i, node; this.nodes )
            {
                ulong records, bytes;
                auto node_queried = node.channels.length > 0;

                foreach ( ch; node.channels )
                {
                    records += ch.records;
                    bytes += ch.bytes;
                }

                char[] node_name = node.address ~ ":" ~ Integer.toString(node.port);

                this.outputSizeRow(i, node_name, longest_node_name, node_queried, records, bytes);
            }
        }
    }


    /***************************************************************************
    
        Queries dht for channel names. Also finds the longest name among those
        returned.
    
        Params:
            dht = dht client to perform query with
            channel_names = array to receive channel names
            longest_channel_name = number to receive the length of the longest
                channel name
    
    ***************************************************************************/

    private void getChannelNames ( ref char[][] channel_names, out size_t longest_channel_name )
    {
        super.dht.assign(super.dht.getChannels(
                ( DhtClient.RequestContext context, char[] channel )
                {
                    if ( channel.length && !channel_names.contains(channel) )
                    {
                        channel_names.appendCopy(channel);
                        if ( channel.length > longest_channel_name )
                        {
                            longest_channel_name = channel.length;
                        }
                    }
                }, &this.notifier));

        super.epoll.eventLoop();

        channel_names.sort;
    }


    /***************************************************************************
    
        Queries dht for size of specified channel.
    
        Params:
            dht = dht client to perform query with
            channel = channel to query
    
    ***************************************************************************/

    private void getChannelSize ( char[] channel )
    {
        super.dht.assign(super.dht.getChannelSize(channel,
                ( DhtClient.RequestContext context, char[] address, ushort port, char[] channel, ulong records, ulong bytes )
                {
                    auto node = this.findNode(address, port);
                    assert(node, typeof(this).stringof ~ "Node mismatch!");

                    node.setChannelSize(channel, records, bytes);
                }, &this.notifier));
        super.epoll.eventLoop();

        Stdout.flush();
    }


    /***************************************************************************
    
        Outputs a hash range info row to Stdout.
    
        Params:
            num = number to prepend to row
            name = name of row item
            longest_name = length of the longest string of type name, used to
                work out how wide the name column needs to be
            range_queried = true if node hash range was successfully queried
            min = min hash
            max = mas hash
    
    ***************************************************************************/

    private void outputHashRangeRow ( uint num, char[] name, size_t longest_name, bool range_queried, hash_t min, hash_t max )
    {
        char[] pad;
        pad.length = longest_name - name.length;
        pad[] = ' ';

        if ( range_queried )
        {
            Stdout.formatln("  {,3}: {}{}   0x{:X8} .. 0x{:X8}", num, name, pad, min, max);
        }
        else
        {
            Stdout.formatln("  {,3}: {}{}   <node did not respond>", num, name, pad);
        }
    }


    /***************************************************************************
    
        Outputs a connections info row to Stdout.
    
        Params:
            num = number to prepend to row
            name = name of row item
            longest_name = length of the longest string of type name, used to
                work out how wide the name column needs to be
            node_queried = true if node connections were successfully queried
            connections = number of connections

    ***************************************************************************/

    private void outputConnectionsRow ( uint num, char[] name, size_t longest_name, bool node_queried, uint connections )
    {
        char[] pad;
        pad.length = longest_name - name.length;
        pad[] = ' ';

        if ( node_queried )
        {
            char[] connections_str;
            DigitGrouping.format(connections, connections_str);
    
            Stdout.formatln("  {,3}: {}{} {,5} connections", num, name, pad, connections_str);
        }
        else
        {
            Stdout.formatln("  {,3}: {}{} <node did not respond>", num, name, pad);
        }
    }


    /***************************************************************************

        Outputs a size info row to Stdout.

        Params:
            num = number to prepend to row
            name = name of row item
            longest_name = length of the longest string of type name, used to
                work out how wide the name column needs to be
            node_queried = true if the node responded to the size requests
            records = number of records
            bytes = number of bytes

    ***************************************************************************/

    private void outputSizeRow ( uint num, char[] name, size_t longest_name, bool node_queried, ulong records, ulong bytes )
    {
        char[] pad;
        pad.length = longest_name - name.length;
        pad[] = ' ';

        if ( node_queried )
        {
            char[] records_str;
            DigitGrouping.format(records, records_str);
    
            char[] bytes_str;
            DigitGrouping.format(bytes, bytes_str);
    
            Stdout.formatln("  {,3}: {}{} {,17} records {,17} bytes", num, name, pad, records_str, bytes_str);
        }
        else
        {
            Stdout.formatln("  {,3}: {}{}    <node did not respond>", num, name, pad);
        }
    }


    /***************************************************************************
    
        Outputs a sum row to Stdout.
    
        Params:
            records = number of records
            bytes = number of bytes
    
    ***************************************************************************/

    private void outputSizeTotal ( size_t longest_name, ulong records, ulong bytes )
    {
        char[] pad;
        pad.length = longest_name;
        pad[] = ' ';

        char[] records_str;
        DigitGrouping.format(records, records_str);

        char[] bytes_str;
        DigitGrouping.format(bytes, bytes_str);

        Stdout.formatln("Total: {} {,17} records {,17} bytes", pad, records_str, bytes_str);
    }


    /***************************************************************************
    
        Finds a node matching the provided address and port in the list of
        nodes.

        Params:
            address = address to match
            port = port to match

        Returns:
            pointer to matched NodeInfo struct in this.nodes, may be null if no
            match found
    
    ***************************************************************************/

    private NodeInfo* findNode ( char[] address, ushort port )
    {
        NodeInfo* found = null;

        foreach ( ref node; this.nodes )
        {
            if ( node.address == address && node.port == port )
            {
                found = &node;
                break;
            }
        }

        return found;
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

