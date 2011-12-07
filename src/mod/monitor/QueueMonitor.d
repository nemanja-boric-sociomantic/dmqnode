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

        Queue client.

    ***************************************************************************/

    private QueueClient queue;


    /***************************************************************************

        Struct storing information about a single queue node.

    ***************************************************************************/

    private struct NodeInfo
    {
        /***********************************************************************

            Node address and port.

        ***********************************************************************/

        char[] address;
        ushort port;


        /***********************************************************************

            Number of connections the node is handling.

        ***********************************************************************/

        uint connections;


        /***********************************************************************

            The per-channel size limit set for the node.

        ***********************************************************************/

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


        /***********************************************************************

            List of channel size infos

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
    }


    /***************************************************************************

        List of node infos

    ***************************************************************************/

    private NodeInfo[] nodes;


    /***************************************************************************

        List of channel names

    ***************************************************************************/

    private char[][] channels;


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
        this.queue = new QueueClient(epoll);
        this.queue.addNodes(args.getString("source"));

        void notifier ( QueueClient.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                Stderr.formatln("Error while performing {} request: {} ({})",
                        *QueueConst.Command.description(info.command),
                        info.exception, info.status);
            }
        }

        void getChannelsDg ( QueueClient.RequestContext c, char[] channel )
        {
            if ( channel.length && !this.channels.contains(channel) )
            {
                this.channels.appendCopy(channel);
            }
        }

        void getChannelSizeDg ( QueueClient.RequestContext c, char[] node_address, ushort node_port, char[] channel, ulong records, ulong bytes )
        {
            auto node = this.findNode(node_address, node_port, true);
            node.setChannelSize(channel, records, bytes);
        }

        void getSizeLimitDg ( QueueClient.RequestContext c, char[] node_address, ushort node_port, ulong bytes )
        {
            auto node = this.findNode(node_address, node_port, true);
            node.channel_size_limit = bytes;
        }

        void getNumConnectionsDg ( QueueClient.RequestContext c, char[] node_address, ushort node_port, size_t conns )
        {
            auto node = this.findNode(node_address, node_port, true);
            node.connections = conns - 1;
        }

        do
        {
            this.queue.assign(this.queue.getChannels(&getChannelsDg, &notifier));
            this.epoll.eventLoop;
    
            this.channels.sort;
    
            this.queue.assign(this.queue.getNumConnections(&getNumConnectionsDg, &notifier));
            this.epoll.eventLoop;
    
            this.queue.assign(this.queue.getSizeLimit(&getSizeLimitDg, &notifier));
            this.epoll.eventLoop;
    
            foreach ( channel; this.channels )
            {
                this.queue.assign(this.queue.getChannelSize(channel, &getChannelSizeDg, &notifier));
            }
            this.epoll.eventLoop;
    
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
        foreach ( node; this.nodes )
        {
            Stdout.bold.cyan.format("{}:{}:", node.address, node.port).bold(false).default_colour;
            foreach ( channel; node.channels )
            {
                if ( node.channel_size_limit > 0 )
                {
                    Stdout.format(" {}: ", channel.name);

                    float percent = (cast(float)channel.bytes / cast(float)node.channel_size_limit) * 100;

                    bool coloured = channel.bytes > 0;
                    if ( coloured )
                    {
                        Stdout.bold;
                        if ( percent >= 50.0 )
                        {
                            Stdout.red;
                        }
                        else
                        {
                            Stdout.green;
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
                    Stdout.format(" {}: {}", channel.name, channel.records);
                }
            }
            Stdout.clearline.cr.flush;

            // TODO: this carriage return logic won't work for multiple nodes
        }
    }


    /***************************************************************************

        Displays results in a detailed format, showing the connections per node,
        and the total size of each channel over all nodes.

    ***************************************************************************/

    private void fullDisplay ( )
    {
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
        foreach ( channel; this.channels )
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

