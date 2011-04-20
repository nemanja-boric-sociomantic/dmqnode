module mod.monitor.QueueMonitor;

private import ocean.core.Array : copy, appendCopy;

private import ocean.text.Arguments;

private import ocean.text.util.DigitGrouping;

private import swarm.queue.QueueClient;

private import tango.core.Array : contains;

private import tango.io.Stdout;


class QueueMonitor
{
    /***********************************************************************
    
        Singleton instance of this class, used in static methods.
    
    ***********************************************************************/

    private static typeof(this) singleton;

    static private typeof(this) instance ( )
    {
        if ( !singleton )
        {
            singleton = new typeof(this);
        }

        return singleton;
    }


    /***********************************************************************

        Parses and validates command line arguments.

        Params:
            args = arguments object
            arguments = command line args (excluding the file name)

        Returns:
            true if the arguments are valid

    ***********************************************************************/

    static public bool parseArgs ( Arguments args, char[][] arguments )
    {
        return instance().validateArgs(args, arguments);
    }


    /***********************************************************************
    
        Main run method, called by OceanException.run.
        
        Params:
            args = processed arguments
    
        Returns:
            always true
    
    ***********************************************************************/

    static public bool run ( Arguments args )
    {
        instance().process(args);
        return true;
    }

    
    private bool validateArgs ( Arguments args, char[][] arguments )
    {
        args("source").required.params(1).aliased('S').help("config file listing queue nodes to connect to");

        if ( arguments.length && !args.parse(arguments) )
        {
            Stderr.formatln("Invalid arguments");
            return false;
        }

        if ( !args.exists("source") )
        {
            Stderr.formatln("Specify the config file to read node info from using -S");
            return false;
        }

        return true;
    }

    private QueueClient queue;

    private struct NodeInfo
    {
        char[] address;
        ushort port;
        uint connections;
        ulong channel_size_limit;

        /***********************************************************************

            Size info for a single channel in a queue node

        ***********************************************************************/

        public struct ChannelInfo
        {
            public char[] name;
            public ulong records;
            public ulong bytes;
        }

        public ChannelInfo[] channels;

    
        /***************************************************************************
        
            Sets the size for a channel.
        
            Params:
                channel = channel name
                records = number of records in channel
                bytes = number of bytes in channel
        
        ***************************************************************************/
        
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


        /***************************************************************************

            Gets the size for a channel into the provided output variables.

            Params:
                channel = channel name
                records = receives the number of records in channel
                bytes = receives the number of bytes in channel

        ***************************************************************************/
        
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
    }

    private NodeInfo[] nodes;

    private void process ( Arguments args )
    {
        this.queue = new QueueClient;
        this.queue.addNodes(args.getString("source"));

        char[][] channels;

        void getChannelsDg ( hash_t id, char[] channel )
        {
            if ( channel.length && !channels.contains(channel) )
            {
                channels.appendCopy(channel);
            }
        }

        void getChannelSizeDg ( hash_t id, char[] node_address, ushort node_port, char[] channel, ulong records, ulong bytes )
        {
            auto node = this.findNode(node_address, node_port, true);
            node.setChannelSize(channel, records, bytes);
        }

        void getSizeLimitDg ( hash_t id, char[] node_address, ushort node_port, ulong bytes )
        {
            auto node = this.findNode(node_address, node_port, true);
            node.channel_size_limit = bytes;
        }

        void getNumConnectionsDg ( hash_t id, char[] node_address, ushort node_port, size_t conns )
        {
            auto node = this.findNode(node_address, node_port, true);
            node.connections = conns - 1;
        }

        this.queue.getChannels(&getChannelsDg).eventLoop;
        channels.sort;

        this.queue.getNumConnections(&getNumConnectionsDg).eventLoop;

        this.queue.getSizeLimit(&getSizeLimitDg).eventLoop;

        foreach ( channel; channels )
        {
            this.queue.getChannelSize(channel, &getChannelSizeDg).eventLoop;
        }

        // Node output
        Stdout.formatln("Nodes:\n----------------------------------------------------------------------------------------------------------");
        Stdout.formatln("{,15} | {,5} | {,3}", "Address", "Port", "Connections");
        Stdout.formatln("----------------------------------------------------------------------------------------------------------");
        foreach ( node; this.nodes )
        {
            Stdout.formatln("{,15} | {,5} | {,3}", node.address, node.port, node.connections);
        }

        // Channel output
        Stdout.formatln("\nChannels:\n----------------------------------------------------------------------------------------------------------");
        Stdout.formatln("{,15} | {,7} | {,12} | {,12} | {,12}", "Name", "% full", "Records", "Bytes", "Bytes free");
        Stdout.formatln("----------------------------------------------------------------------------------------------------------");
        foreach ( channel; channels )
        {
            ulong records, bytes, size_limit;
            foreach ( node; this.nodes )
            {
                size_limit += node.channel_size_limit;
                ulong channel_records, channel_bytes;
                node.getChannelSize(channel, channel_records, channel_bytes);
                records += channel_records;
                bytes += channel_bytes;
            }

            double percent = size_limit > 0 ? (cast(double)bytes / cast(double)size_limit) * 100 : 0;

            char[] records_str;
            DigitGrouping.format(records, records_str);

            char[] bytes_str;
            DigitGrouping.format(bytes, bytes_str);

            Stdout.format("{,15} | {,6}% | {,12} | {,12} | ", channel, percent, records_str, bytes_str);

            if ( size_limit > 0 )
            {
                char[] size_limit_str;
                DigitGrouping.format(size_limit - bytes, size_limit_str);
                Stdout.formatln("{,12}", size_limit_str);
            }
            else
            {
                Stdout.formatln("{,12}", "unlimited");
            }
        }
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
    
    private NodeInfo* findNode ( char[] address, ushort port, bool add_if_new )
    {
        NodeInfo* found = null;
    
        foreach ( ref node; this.nodes )
        {
            if ( node.address == address && node.port == port )
            {
                return &node;
            }
        }

        if ( add_if_new )
        {
            this.nodes.length = this.nodes.length + 1;
            found = &this.nodes[$-1];
            found.address.copy(address);
            found.port = port;
        }

        return found;
    }
}

// Channel info:
// Name     %full    records     bytes    bytes free



