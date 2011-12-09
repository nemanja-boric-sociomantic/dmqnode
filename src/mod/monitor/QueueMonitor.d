/*******************************************************************************

    Queue monitor

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.monitor.QueueMonitor;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.monitor.Tables;

private import ocean.core.Array : appendCopy;

private import ocean.io.Stdout;

private import ocean.io.select.EpollSelectDispatcher;

private import ocean.text.Arguments;

private import swarm.queue.QueueClient;
private import swarm.queue.QueueConst;

private import tango.core.Array : contains;

private import tango.core.Thread;

private import tango.io.FilePath;



/*******************************************************************************

    Queue monitor class

*******************************************************************************/

public class QueueMonitor
{
    /***************************************************************************

        Epoll select dispatcher.

    ***************************************************************************/

    private EpollSelectDispatcher epoll;


    /***************************************************************************

        Class containing data and methods for a single queue being monitored.

    ***************************************************************************/

    private class Queue
    {
        /***********************************************************************
    
            Struct storing information about a single queue node.
    
        ***********************************************************************/
    
        private struct NodeInfo
        {
            /*******************************************************************
    
                Node address and port.
    
            *******************************************************************/
    
            char[] address;
            ushort port;
    
    
            /*******************************************************************
    
                Number of connections the node is handling.

            *******************************************************************/
    
            uint connections;
    
    
            /*******************************************************************
    
                The per-channel size limit set for the node.
    
            *******************************************************************/
    
            ulong channel_size_limit;
    
    
            /*******************************************************************
    
                Size info for a single channel in a queue node
    
            *******************************************************************/
    
            public struct ChannelInfo
            {
                public char[] name;
                public ulong records, last_records;
                public ulong bytes;

                public int records_diff ( )
                {
                    return this.records - this.last_records;
                }
            }
    
    
            /*******************************************************************
    
                List of channel size infos
    
            *******************************************************************/
    
            public ChannelInfo[] channels;
    
        
            /*******************************************************************
            
                Sets the size for a channel.
            
                Params:
                    channel = channel name
                    records = number of records in channel
                    bytes = number of bytes in channel
            
            *******************************************************************/
            
            public void setChannelSize ( char[] channel, ulong records, ulong bytes )
            {
                foreach ( ref ch; this.channels )
                {
                    if ( ch.name == channel )
                    {
                        ch.last_records = ch.records;

                        ch.records = records;
                        ch.bytes = bytes;
                        return;
                    }
                }
    
                this.channels ~= ChannelInfo("", records, bytes);
                this.channels[$-1].name.copy(channel);
            }
    
    
            /*******************************************************************
    
                Gets the size for a channel into the provided output variables.
    
                Params:
                    channel = channel name
                    records = receives the number of records in channel
                    bytes = receives the number of bytes in channel
                    diff = receives the difference in records from the last run
    
            *******************************************************************/
    
            public void getChannelSize ( char[] channel, out ulong records, out ulong bytes )
            {
                foreach ( ref ch; this.channels )
                {
                    if ( ch.name == channel )
                    {
                        records = ch.records;
                        bytes = ch.bytes;
                        return;
                    }
                }
            }
        }


        /***********************************************************************

            Queue client used to connect to this queue.

        ***********************************************************************/

        public QueueClient client;


        /***********************************************************************

            Names of channels in this queue.

        ***********************************************************************/

        public char[][] channels;


        /***********************************************************************

            Array of information about nodes in this queue.

        ***********************************************************************/

        public NodeInfo[] nodes;


        /***********************************************************************

            Filepath to the ini file defining the nodes in this queue.

        ***********************************************************************/

        private FilePath filepath;


        /***********************************************************************

            Constructor.

            Params:
                epoll = epoll selector to use with the queue client
                ini = path of queue nodes ini file

        ***********************************************************************/

        public this ( EpollSelectDispatcher epoll, char[] ini )
        {
            this.filepath = new FilePath(ini);
            this.client = new QueueClient(epoll);
            this.client.addNodes(ini);
        }


        /***********************************************************************

            Assigns a GetChannels command to this queue.

        ***********************************************************************/

        public void getChannels ( )
        {
            this.client.assign(this.client.getChannels(&this.getChannelsDg, &this.notifier));
        }


        /***********************************************************************

            Assigns a GetChannelSize command to each channel of this queue.

        ***********************************************************************/

        public void getChannelSizes ( )
        {
            foreach ( channel; this.channels )
            {
                this.client.assign(this.client.getChannelSize(channel, &this.getChannelSizeDg, &this.notifier));
            }
        }


        /***********************************************************************

            Assigns a GetSizeLimit command to this queue.

        ***********************************************************************/

        public void getSizeLimit ( )
        {
            this.client.assign(this.client.getSizeLimit(&this.getSizeLimitDg, &this.notifier));
        }


        /***********************************************************************

            Assigns a GetNumConnections command to this queue.

        ***********************************************************************/

        public void getNumConnections ( )
        {
            this.client.assign(this.client.getNumConnections(&this.getNumConnectionsDg, &this.notifier));
        }


        /***********************************************************************

            Returns:
                identifier string for this queue (the filename of the ini file)

        ***********************************************************************/

        public char[] id ( )
        {
           return this.filepath.name;
        }


        /***********************************************************************

            Get channels callback.

            Params:
                c = request context (unused)
                channel = name of channel in queue

        ***********************************************************************/

        private void getChannelsDg ( QueueClient.RequestContext c, char[] channel )
        {
            if ( channel.length )
            {
                if ( !this.channels.contains(channel) )
                {
                    this.channels.appendCopy(channel);
                }
            }
            else
            {
                // Sort channels list once all received
                this.channels.sort;
            }
        }


        /***********************************************************************

            Get channel size callback.

            Params:
                c = request context (unused)
                node_address = address of node
                node_port = port of node
                channel = name of channel in queue
                records = records in channel
                bytes = bytes in channel

        ***********************************************************************/

        private  void getChannelSizeDg ( QueueClient.RequestContext c,
                char[] node_address, ushort node_port,
                char[] channel, ulong records, ulong bytes )
        {
            auto node = this.findNode(node_address, node_port, true);
            node.setChannelSize(channel, records, bytes);
        }


        /***********************************************************************

            Get size limit callback.

            Params:
                c = request context (unused)
                node_address = address of node
                node_port = port of node
                bytes = size limit

        ***********************************************************************/

        private  void getSizeLimitDg ( QueueClient.RequestContext c,
                char[] node_address, ushort node_port, ulong bytes )
        {
            auto node = this.findNode(node_address, node_port, true);
            node.channel_size_limit = bytes;
        }


        /***********************************************************************

            Get num connections callback.

            Params:
                c = request context (unused)
                node_address = address of node
                node_port = port of node
                conns = handled connections

        ***********************************************************************/

        private  void getNumConnectionsDg ( QueueClient.RequestContext c,
                char[] node_address, ushort node_port, size_t conns )
        {
            auto node = this.findNode(node_address, node_port, true);
            node.connections = conns - 1;
        }


        /***********************************************************************

            Queue client request notification callback.

            Params:
                info = information about an event which occurred.

        ***********************************************************************/

        private void notifier ( QueueClient.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                Stderr.formatln("Error while performing {} request: {} ({})",
                        *QueueConst.Command.description(info.command),
                        info.exception, info.status);
            }
        }


        /***********************************************************************

            Finds a node matching the provided address and port in the list of
            nodes.

            Params:
                address = address to match
                port = port to match

            Returns:
                pointer to matched NodeInfo struct in this.nodes, may be null if
                no match found

        ***********************************************************************/

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


    /***************************************************************************

        List of queues being monitored, indexed by name of ini file.

    ***************************************************************************/

    private Queue[char[]] queues;


    /***************************************************************************

        String buffer used for formatting column padding.

    ***************************************************************************/

    private char[] padding;


    /***************************************************************************

        Length of the longest queue id.

    ***************************************************************************/

    private size_t longest_queue_id;


    /***************************************************************************

        Initialises a queue client and connects to the nodes specified in the
        command line arguments. Gets information from all connected queue nodes
        and displays it in two tables.

        Params:
            args = processed arguments

    ***************************************************************************/

    public void run ( Arguments args )
    {
        this.epoll = new EpollSelectDispatcher;

        foreach ( ini; args("source").assigned )
        {
            if ( !(ini in this.queues ) )
            {
                auto queue = new Queue(this.epoll, ini);

                this.queues[ini] = queue;

                if ( queue.id.length > this.longest_queue_id )
                {
                    this.longest_queue_id = queue.id.length;
                }
            }
        }
        assert(this.queues.length);

        do
        {
            if ( args.exists("minimal") )
            {
                this.minimalDisplay();
            }
            else
            {
                this.fullDisplay();
            }

            Thread.sleep(args.getInt!(int)("periodic"));
        }
        while ( args.exists("periodic") );
    }


    /***************************************************************************

        Displays results in a minimal format, just showing the size of each
        channel per node.

    ***************************************************************************/

    private void minimalDisplay ( )
    {
        foreach ( queue; this.queues )
        {
            queue.getChannels;
            queue.getSizeLimit;
        }
        this.epoll.eventLoop;

        foreach ( queue; this.queues )
        {
            queue.getChannelSizes;
        }
        this.epoll.eventLoop;

        foreach ( queue; this.queues )
        {
            foreach ( i, node; queue.nodes )
            {
                if ( this.queues.length > 1 )
                {
                    this.padding.length = this.longest_queue_id - queue.id.length;
                    this.padding[] = ' ';

                    Stdout.bold.magenta.format("{}{}:", this.padding, queue.id)
                        .bold(false).default_colour;
                }

                if ( queue.nodes.length > 1 )
                {
                    // TODO: do we need to display the addr/port? or is the number enough?
//                    Stdout.bold.cyan.format(" {}:{}:", node.address, node.port).bold(false).default_colour;
                    Stdout.bold.cyan.format(" {,2}:", i).bold(false).default_colour;
                }

                foreach ( channel; node.channels )
                {
                    if ( node.channel_size_limit > 0 )
                    {
                        Stdout.format(" {} ", channel.name);

                        float percent = (cast(float)channel.bytes / cast(float)node.channel_size_limit) * 100;

                        bool coloured = channel.records_diff != 0 || percent > 50.0;
                        if ( coloured )
                        {
                            Stdout.bold;
                            if ( percent >= 50.0 )
                            {
                                Stdout.red;
                            }
                            else
                            {
                                channel.records_diff > 0 ? Stdout.yellow : Stdout.green;
                            }
                        }
    
                        Stdout.format("{}%", percent);
    
                        if ( coloured )
                        {
                            Stdout.bold(false).default_colour;
                        }
                    }
                    else
                    {
                        Stdout.format(" {} {}", channel.name, channel.records);
                    }
                }
                Stdout.newline.flush;

                // TODO: clever cursor resetting logic
            }
        }
    }


    /***************************************************************************

        Displays results in a detailed format, showing the connections per node,
        and the total size of each channel over all nodes.

    ***************************************************************************/

    private void fullDisplay ( )
    {
        foreach ( queue; this.queues )
        {
            queue.getChannels;
            queue.getNumConnections;
            queue.getSizeLimit;
        }
        this.epoll.eventLoop;

        foreach ( queue; this.queues )
        {
            queue.getChannelSizes;
        }
        this.epoll.eventLoop;

        foreach ( queue; this.queues )
        {
            // Nodes table
            Stdout.formatln("Nodes:");
    
            scope nodes_table = new Table(3);
    
            nodes_table.firstRow.setDivider();
            nodes_table.nextRow.set(Table.Cell.String("Address"), Table.Cell.String("Port"), Table.Cell.String("Connections"));
            nodes_table.nextRow.setDivider();
            foreach ( node; queue.nodes )
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
            foreach ( channel; queue.channels )
            {
                ulong records, bytes, size_limit;
                foreach ( node; queue.nodes )
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
    }
}

