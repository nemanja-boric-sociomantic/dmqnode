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

    Inherited from super class:
        -h = display help

*******************************************************************************/

module src.mod.info.DhtInfo;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.model.DhtTool;

private import swarm.dht2.DhtClient,
               swarm.dht2.DhtHash,
               swarm.dht2.DhtConst;

private import ocean.core.Array;

private import ocean.text.Arguments;

private import tango.core.Array;

private import tango.io.Stdout;

private import Integer = tango.text.convert.Integer;



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
    
        Size info for a single node in a dht
    
    ***************************************************************************/

    private struct NodeInfo
    {
        /***********************************************************************
        
            Node address & port
        
        ***********************************************************************/

        public char[] address;
        public ushort port;


        /***********************************************************************
        
            Node hash range
        
        ***********************************************************************/

        public bool range_queried;
        public hash_t min_hash;
        public hash_t max_hash;

        
        /***********************************************************************

            Number of connections handled by node

        ***********************************************************************/

        public size_t connections;


        /***********************************************************************
        
            Size info for a single channel in a dht node
        
        ***********************************************************************/

        public struct ChannelInfo
        {
            public char[] name;
            public ulong records;
            public ulong bytes;
        }


        /***********************************************************************
        
            Array of info on channels in a dht node
        
        ***********************************************************************/

        public ChannelInfo[] channels;


        /***********************************************************************

            Sets the size for a channel.

            Params:
                channel = channel name
                records = number of records in channel
                bytes = number of bytes in channel

        ***********************************************************************/

        public void setChannelSize ( char[] channel, ulong records, ulong bytes )
        {
            foreach ( ref ch; this.channels )
            {
                if ( ch.name == channel )
                {
                    ch.records = records;
                    ch.bytes = bytes;
                    return;
                }
            }

            this.channels ~= ChannelInfo("", records, bytes);
            this.channels[$-1].name.copy(channel);
        }


        /***********************************************************************

            Gets the size for a channel into the provided output variables.

            Params:
                channel = channel name
                records = receives the number of records in channel
                bytes = receives the number of bytes in channel
                node_queried = receives the boolean telling whether the node
                    responded to the query to get the size of this channel

        ***********************************************************************/

        public void getChannelSize ( char[] channel, out ulong records, out ulong bytes, out bool node_queried )
        {
            foreach ( ref ch; this.channels )
            {
                if ( ch.name == channel )
                {
                    records += ch.records;
                    bytes += ch.bytes;
                    node_queried = true;
                    return;
                }
            }
        }


        /***********************************************************************

            Formats the provided string with the name of this node.

            Params:
                name = string to receive node name

        ***********************************************************************/

        public void name ( ref char[] name )
        {
            typeof(*this).formatName(this.address, this.port, name);
        }


        /***********************************************************************

            Formats the provided string with the name of the specified node.
    
            Params:
                address = node ip address
                port = node port
                name = string to receive node name

        ***********************************************************************/

        static public void formatName ( char[] address, ushort port, ref char[] name )
        {
            name.concat(address, ":", Integer.toString(port));
        }
    }


    /***************************************************************************

        Array of info on all dht nodes

    ***************************************************************************/

    private NodeInfo[] nodes;


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

    override protected void dhtError ( DhtClient.ErrorInfo e )
    {
        super.dht_error = true;
        this.dht_errors.appendCopy(e.message);
    }


    /***************************************************************************

        Main process method. Runs the tool based on the passed command line
        arguments.
    
        Params:
            dht = dht client to use
    
    ***************************************************************************/
    
    protected void process_ ( DhtClient dht )
    {
        // Get node addresses/ports
        size_t longest_node_name;

        foreach ( node; dht )
        {
            this.nodes ~= NodeInfo(node.nodeitem.Address, node.nodeitem.Port,
                    node.nodeitem.isResponsibleRangeQueried, node.nodeitem.MinValue, node.nodeitem.MaxValue);

            auto name_len = node.nodeitem.Address.length + 6;
            if ( name_len > longest_node_name )
            {
                longest_node_name = name_len;
            }
        }

        // Display various forms of output
        if ( this.data )
        {
            this.displayContents(dht, longest_node_name);
        }

        if ( this.connections )
        {
            this.displayNumConnections(dht, longest_node_name);
        }

        if ( this.api_version )
        {
            this.displayApiVersions(dht, longest_node_name);
        }

        if ( this.hash_ranges )
        {
            this.displayHashRanges(dht, longest_node_name);
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
    }


    /***************************************************************************
    
        Checks whether the parsed command line args are valid.
    
        Params:
            args = command line arguments object to validate
    
        Returns:
            true if args are valid
    
    ***************************************************************************/
    
    override protected bool validArgs ( Arguments args )
    {
        if ( !args.exists("source") )
        {
            Stderr.formatln("No xml source file specified (use -S)");
            return false;
        }

        return true;
    }
    
    
    /***************************************************************************
    
        Initialises this instance from the specified command line args.
    
        Params:
            args = command line arguments object to read settings from
    
    ***************************************************************************/
    
    override protected void readArgs ( Arguments args )
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
            this.data = true;
        }
    }

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
    
    private void displayHashRanges ( DhtClient dht, size_t longest_node_name )
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
    
    private void displayApiVersions ( DhtClient dht, size_t longest_node_name )
    {
        Stdout.formatln("\nApi version:");
        Stdout.formatln("------------------------------------------------------------------------------");

        bool output;
        dht.getVersion(
                ( DhtClient.RequestContext context, char[] api_version )
                {
                    if ( api_version.length && !output )
                    {
                        Stdout.formatln("  API: {}", api_version);
                        output = true;
                    }
                }).eventLoop;
    }


    /***************************************************************************

        Queries and displays the number of connections being handled per node.
    
        Params:
            dht = dht client to perform query with
            longest_node_name = the length of the longest node name string

    ***************************************************************************/

    private void displayNumConnections ( DhtClient dht, size_t longest_node_name )
    {
        Stdout.formatln("\nConnections being handled:");
        Stdout.formatln("------------------------------------------------------------------------------");

        // Set the number of connections for all nodes to an invalid value
        foreach ( ref node; this.nodes )
        {
            node.connections = size_t.max;
        }

        // Query all nodes for their active connections
        dht.getNumConnections(
                ( DhtClient.RequestContext context, char[] node_address, ushort node_port, size_t num_connections )
                {
                    auto node = this.findNode(node_address, node_port);
                    assert(node, typeof(this).stringof ~ "Node mismatch!");

                    node.connections = num_connections;
                }).eventLoop;

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

        Queries and displays the size of the contents of each channel and node.
    
        Params:
            dht = dht client to perform query with
            longest_node_name = the length of the longest node name string

    ***************************************************************************/

    private void displayContents ( DhtClient dht, size_t longest_node_name )
    {
        // Get channel names
        size_t longest_channel_name;
        char[][] channel_names;

        this.getChannelNames(dht, channel_names, longest_channel_name);

        // Get channel size info
        foreach ( channel; channel_names )
        {
            this.getChannelSize(dht, channel);
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

    private void getChannelNames ( DhtClient dht, ref char[][] channel_names, out size_t longest_channel_name )
    {
        dht.getChannels(
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
                }
            ).eventLoop();
    
        channel_names.sort;
    }


    /***************************************************************************
    
        Queries dht for size of specified channel.
    
        Params:
            dht = dht client to perform query with
            channel = channel to query
    
    ***************************************************************************/

    private void getChannelSize ( DhtClient dht, char[] channel )
    {
        dht.getChannelSize(channel,
                ( DhtClient.RequestContext context, char[] address, ushort port, char[] channel, ulong records, ulong bytes )
                {
                    auto node = this.findNode(address, port);
                    assert(node, typeof(this).stringof ~ "Node mismatch!");

                    node.setChannelSize(channel, records, bytes);
                }).eventLoop();

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
            formatCommaNumber(connections, connections_str);
    
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
            formatCommaNumber(records, records_str);
    
            char[] bytes_str;
            formatCommaNumber(bytes, bytes_str);
    
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
        formatCommaNumber(records, records_str);

        char[] bytes_str;
        formatCommaNumber(bytes, bytes_str);

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
        if ( this.dht_errors.length )
        {
            Stderr.formatln("\nDht errors which occurred during operation:");
            Stderr.formatln("------------------------------------------------------------------------------");

            foreach ( i, err; this.dht_errors )
            {
                Stderr.formatln("  {,3}: {}", i, err);
            }
        }
    }


    /***************************************************************************

        Formats a number to a string, with comma separation every 3 digits

    ***************************************************************************/

    private static char[] formatCommaNumber ( T ) ( T num, ref char[] output )
    {
        output.length = 0;
    
        // Format number into a string
        char[20] string_buf;
        auto string = Integer.format(string_buf, num);
    
        bool comma;
        size_t left = 0;
        size_t right = left + 3;
        size_t first_comma;
    
        // Handle negative numbers
        if ( string[0] == '-' )
        {
            output ~= "-";
            string = string[1..$];
        }
    
        // Find position of first comma
        if ( string.length > 3 )
        {
            comma = true;
            first_comma = string.length % 3;
    
            if ( first_comma > 0 )
            {
                right = first_comma;
            }
        }
    
        // Copy chunks of the formatted number into the destination string, with commas
        do
        {
            if ( right >= string.length )
            {
                right = string.length;
                comma = false;
            }
            output ~= string[left..right];
            if ( comma )
            {
                output ~= ",";
            }
    
            left = right;
            right = left + 3;
        }
        while( left < string.length );
    
        return output;
    }
}

