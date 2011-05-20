module mod.monitor.QueueMonitor;

private import mod.monitor.Tables;

private import ocean.core.Array : appendCopy;

private import ocean.io.select.EpollSelectDispatcher;

private import ocean.text.Arguments;

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

    private EpollSelectDispatcher epoll;

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
        this.epoll = new EpollSelectDispatcher;
        this.queue = new QueueClient(epoll);
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

        this.queue.getChannels(&getChannelsDg);
        this.epoll.eventLoop;

        channels.sort;

        this.queue.getNumConnections(&getNumConnectionsDg);
        this.epoll.eventLoop;

        this.queue.getSizeLimit(&getSizeLimitDg);
        this.epoll.eventLoop;

        foreach ( channel; channels )
        {
            this.queue.getChannelSize(channel, &getChannelSizeDg);
            this.epoll.eventLoop;
        }

        // Nodes table
        Stdout.formatln("Nodes:");

        scope nodes_table = new Table(3);

        nodes_table.firstRow.setDivider();
        nodes_table.nextRow.set(Table.Cell.String("Address"), Table.Cell.String("Port"), Table.Cell.String("Connections"));
        nodes_table.nextRow.setDivider();
        foreach ( node; this.nodes )
        {
            nodes_table.nextRow.set(Table.Cell.String(node.address), Table.Cell.Integer(node.port), Table.Cell.Integer(node.connections));
        }
        nodes_table.nextRow.setDivider();
        nodes_table.display();

        // Channels table
        Stdout.formatln("\nChannels:");

        scope channels_table = new Table(5);

        channels_table.firstRow.setDivider();
        channels_table.nextRow.set(Table.Cell.String("Name"), Table.Cell.String("% full"), Table.Cell.String("Records"), Table.Cell.String("Bytes"), Table.Cell.String("Bytes free"));
        channels_table.nextRow.setDivider();
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

            float percent = size_limit > 0 ? (cast(float)bytes / cast(float)size_limit) * 100 : 0;

            channels_table.nextRow.set(Table.Cell.String(channel), Table.Cell.Float(percent), Table.Cell.Integer(records), Table.Cell.Integer(bytes),
                    size_limit > 0 ? Table.Cell.Integer(size_limit - bytes) : Table.Cell.String("unlimited"));
        }
        channels_table.nextRow.setDivider();
        channels_table.display();
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

