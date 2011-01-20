/*******************************************************************************

    DHT node info

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman

    Display information about a dht - the names of the channels, and optionally
    the number of records & bytes per channel.

    Command line parameters:
        -S = dhtnodes.xml file for dht to query
        -v = verbose output, displays info per channel per node, and per node
            per channel

    Inherited from super class:
        -h = display help

*******************************************************************************/

module src.mod.info.DhtInfo;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.model.DhtTool;

private import swarm.dht.DhtClient,
           swarm.dht.DhtHash,
           swarm.dht.DhtConst;

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
    
        ***********************************************************************/

        public void getChannelSize ( char[] channel, out ulong records, out ulong bytes )
        {
            foreach ( ref ch; this.channels )
            {
                if ( ch.name == channel )
                {
                    records += ch.records;
                    bytes += ch.bytes;
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
            name.concat(this.address, ":", Integer.toString(this.port));
        }
    }


    /***************************************************************************

        Array of info on all dht nodes

    ***************************************************************************/

    private NodeInfo[] nodes;


    /***************************************************************************
    
        Toggles verbose output
    
    ***************************************************************************/
    
    private bool verbose;


    /***************************************************************************

        Main process method. Runs the tool based on the passed command line
        arguments.
    
        Params:
            dht = dht client to use
    
    ***************************************************************************/
    
    protected void process_ ( DhtClient dht )
    {
        // Get channel names
        size_t longest_channel_name;
        char[][] channel_names;

        this.getChannelNames(dht, channel_names, longest_channel_name);

        // Get node addresses/ports
        size_t longest_node_name;

        foreach ( node; dht )
        {
            this.nodes ~= NodeInfo(node.nodeitem.Address, node.nodeitem.Port);
            
            auto name_len = node.nodeitem.Address.length + 6;
            if ( name_len > longest_node_name )
            {
                longest_node_name = name_len;
            }
        }

        // Get channel size info
        foreach ( channel; channel_names )
        {
            this.getChannelSize(dht, channel);
        }

        // Display channels
        if ( verbose )
        {
            Stdout.formatln("\nChannels:");
            Stdout.formatln("------------------------------------------------------------------------------");

            foreach ( i, channel; channel_names )
            {
                Stdout.formatln("Channel {}: {}:", i, channel);

                ulong channel_records, channel_bytes;
                foreach ( j, node; this.nodes )
                {
                    ulong records, bytes;
                    node.getChannelSize(channel, records, bytes);
                    channel_records += records;
                    channel_bytes += bytes;

                    char[] node_name;
                    node.name(node_name);

                    this.outputRow(j, node_name, longest_node_name, records, bytes);
                }

                this.outputTotal(channel_records, channel_bytes);
            }
        }
        else
        {
            Stdout.formatln("Channels:");
            foreach ( i, channel; channel_names )
            {
                ulong records, bytes;
                foreach ( node; this.nodes )
                {
                    node.addChannelSize(channel, records, bytes);
                }
    
                this.outputRow(i, channel, longest_channel_name, records, bytes);
            }
        }

        // Display nodes
        if ( verbose )
        {
            Stdout.formatln("\nNodes:");
            Stdout.formatln("------------------------------------------------------------------------------");

            foreach ( i, node; this.nodes )
            {
                char[] node_name;
                node.name(node_name);
                Stdout.formatln("Node {}: {}:", i, node_name);

                ulong node_records, node_bytes;

                foreach ( j, ch; node.channels )
                {
                    this.outputRow(j, ch.name, longest_channel_name, ch.records, ch.bytes);
                    node_records += ch.records;
                    node_bytes += ch.bytes;
                }

                this.outputTotal(node_records, node_bytes);
            }
        }
        else
        {
            Stdout.formatln("\nNodes:");
            foreach ( i, node; this.nodes )
            {
                ulong records, bytes;
    
                foreach ( ch; node.channels )
                {
                    records += ch.records;
                    bytes += ch.bytes;
                }
    
                char[] node_name = node.address ~ ":" ~ Integer.toString(node.port);
    
                this.outputRow(i, node_name, longest_node_name, records, bytes);
            }
        }
    }


    /***************************************************************************
    
        Adds command line arguments specific to this tool.
        
        Params:
            args = command line arguments object to add to
    
    ***************************************************************************/

    override protected void addArgs_ ( Arguments args )
    {
        args("source").params(1).required().aliased('S').help("path of dhtnodes.xml file defining nodes to query");
        args("verbose").aliased('v').help("verbose output, displays info per channel per node, and per node per channel");
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

        this.verbose = args.getBool("verbose");
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
                ( uint id, char[] channel )
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
                ( hash_t id, char[] address, ushort port, char[] channel, ulong records, ulong bytes )
                {
                    auto node = this.findNode(address, port);
                    assert(node, typeof(this).stringof ~ "Node mismatch!");

                    node.setChannelSize(channel, records, bytes);
                }).eventLoop();

        Stdout.flush();
    }


    /***************************************************************************
    
        Outputs a size info row to Stdout.
    
        Params:
            num = number to prepend to row
            name = name of row item
            longest_name = length of the longest string of type name, used to
                work out how wide the name column needs to be
            records = number of records
            bytes = number of bytes
    
    ***************************************************************************/

    private void outputRow ( uint num, char[] name, size_t longest_name, ulong records, ulong bytes )
    {
        char[] pad;
        pad.length = longest_name - name.length;
        pad[] = ' ';

        char[] records_str;
        formatCommaNumber(records, records_str);

        char[] bytes_str;
        formatCommaNumber(bytes, bytes_str);

        Stdout.formatln("  {,3}: {}{} {,17} records {,17} bytes", num, name, pad, records_str, bytes_str);
    }


    /***************************************************************************
    
        Outputs a sum row to Stdout.
    
        Params:
            records = number of records
            bytes = number of bytes
    
    ***************************************************************************/

    private void outputTotal ( ulong records, ulong bytes )
    {
        char[] records_str;
        formatCommaNumber(records, records_str);
    
        char[] bytes_str;
        formatCommaNumber(bytes, bytes_str);
    
        Stdout.formatln("Total = {} records {} bytes\n", records_str, bytes_str);
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

