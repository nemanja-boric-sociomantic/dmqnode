/*******************************************************************************

    DHT node monitor daemon

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        Jun 2010: Initial release

    authors:        Gavin Norman

    --

    Displays an updating count of the number of records in each channel of the
    DHT node specified in the config file.

 ******************************************************************************/

module src.mod.monitor.DhtNodeMonitor;



/*******************************************************************************

    Imports

 ******************************************************************************/

private import src.core.config.MonitorConfig;

private import swarm.dht.DhtClient,
               swarm.dht.DhtHash,
               swarm.dht.DhtConst;

private import swarm.dht.client.DhtNodesConfig;

private import ocean.core.Array;

private import tango.core.Thread;

private import tango.core.Array;

private import tango.time.Clock;

private import tango.math.Math : min;

private import tango.util.Arguments;

private import tango.util.log.Trace;

private import Integer = tango.text.convert.Integer;



/*******************************************************************************

    DhtNodeMonitor - starts the monitor daemon

 ******************************************************************************/

struct DhtNodeMonitor
{
    static NodeMonDaemon daemon;

    public static bool run ( Arguments args )
    {
        daemon = new NodeMonDaemon();

        if (args.contains("d"))
        {
            daemon.run();
        }
        else
        {
            daemon.update();
        }
        
        return true;
    }
}



/*******************************************************************************

    DHT node monitor daemon

 ******************************************************************************/

class NodeMonDaemon
{
    /***************************************************************************

        Sleep time between updates (in daemon mode)
    
     **************************************************************************/

    private const WAIT_TIME = 10; // seconds
    
    /***************************************************************************

        Dht client
    
     **************************************************************************/

    private DhtClient dhtclient;

    /***************************************************************************

        List of channels in DHT node - created by constructor

     **************************************************************************/

    private char[][] channels;
    
    /***************************************************************************

        Array of error messages, node ip and port are key.
    
     **************************************************************************/

    private char[][char[]] errors;

    /***************************************************************************

        Number of bytes for a channel

     **************************************************************************/

    private ulong[char[]][char[]] channel_bytes;

    /***************************************************************************

        Number of records for a channel

     **************************************************************************/

    private ulong[char[]][char[]] channel_records;

    /***************************************************************************

        Total number of bytes for all channels

     **************************************************************************/

    private ulong[char[]] total_bytes;

    /***************************************************************************

        Total number of records for all channels

     **************************************************************************/

    private ulong[char[]] total_records;

    /***************************************************************************

        Buffer for node id

     **************************************************************************/

    private char[] node_id;

    /***************************************************************************

        NodeItems for display method

     **************************************************************************/

    private DhtConst.NodeItem[] rowNodeItems;

    /***************************************************************************

        Buffer for thousand separator method
    
     **************************************************************************/
    
    private char[] buf;

    /***************************************************************************

        Constructor

     **************************************************************************/

    public this ( )
    {
        this.dhtclient = new DhtClient();

        DhtNodesConfig.addNodesToClient(this.dhtclient, "etc/dhtnodes.xml");
        
        this.dhtclient.queryNodeRanges().eventLoop();

        this.dhtclient.error_callback = &this.onConnectionError;
    }

    /***************************************************************************

        Destructor
    
    ***************************************************************************/

    ~this ( )
    {
        delete this.dhtclient;
    }
    
    /***************************************************************************
    
        Receives error information from the DhtClient
    
    ***************************************************************************/

    void onConnectionError ( DhtClient.ErrorInfo info )
    {
        char[] node_id = info.nodeitem.Address ~ ":" 
                            ~ Integer.toString(info.nodeitem.Port);

        if (!(node_id in this.errors))
        {
            this.errors[info.nodeitem.Address ~ ":" 
                    ~ Integer.toString(info.nodeitem.Port)]  = info.message;
        }
    }
    
    /***************************************************************************

        Daemon main loop. Updates the display, then sleeps a while - on infinite
        loop.

    ***************************************************************************/

    public void run ()
    {
        while (true)
        {
            this.update();
            Thread.sleep(WAIT_TIME);
        }
    }

    /***************************************************************************

        Updates the display. Queries the DHT node for the number of records in
        all channels.

    ***************************************************************************/

    private void update ()
    {
        foreach (k; this.errors.keys)            this.errors.remove(k);
        foreach (k; this.channel_bytes.keys)     this.channel_bytes.remove(k);
        foreach (k; this.total_bytes.keys)       this.total_bytes.remove(k);
        foreach (k; this.channel_records.keys)   this.channel_records.remove(k);
        foreach (k; this.total_records.keys)     this.total_records.remove(k);

        this.dhtclient.getChannels(&this.addChannels).eventLoop();

        foreach (channel; this.channels)
        {
            this.dhtclient.getChannelSize(channel, &this.addChannelSize).eventLoop();
        }

        this.print();
    }

    /***************************************************************************

        Prints all fetched data to Stdout.

    ***************************************************************************/

    private void print ()
    {
        uint col_num, node_num;
        
        auto columns = this.getDisplayColumns();
        auto number_nodes = this.dhtclient.nodeRegistry.length;
        
        this.printTime(columns);

        foreach (node; this.dhtclient)
        {
            col_num++;
            node_num++;
            
            this.rowNodeItems ~= node.nodeitem;

            if (col_num == columns)
            {
                this.printRow();
                
                this.rowNodeItems.length = 0;
                col_num = 0;
            }
        }
        
        this.printRow();
    }

    /***************************************************************************

        Prints one row of data. Row length is determined by the
        "DISPLAY : columns" configuration setting.

    ***************************************************************************/

    private void printRow ()
    {
       Trace.formatln("");
        
       this.printBoxLine(false);

       this.printNodeInfo();

       this.printNodeRange();

       this.printNodeInfoHeaders();

       this.printNodeChannels();

       this.printNodeTotal();
    }

    /***************************************************************************

        Prints the current time and number of nodes.

    ***************************************************************************/

    private void printTime ( size_t columns )
    {
       this.printHeadLine(columns);

        Trace.formatln(" Time: {}            Number of Nodes: {}",
                Clock.now(), this.dhtclient.nodeRegistry.length);

        this.printHeadLine(columns);
    }

    /***************************************************************************

        Prints a list of channels.

    ***************************************************************************/

    private void printNodeChannels () 
    {
        foreach (channel; this.channels)
        {
            Trace.format("{,21} |", channel);

            foreach (node; this.rowNodeItems)
            {
                this.node_id = node.Address ~ ":" ~ Integer.toString(node.Port);
                
                this.printError(this.node_id);

                Trace.format(" | {,11} |",     formatCommaNumber(this.channel_records[this.node_id][channel], this.buf));
                Trace.format(" {,14} bytes |", formatCommaNumber(this.channel_bytes[this.node_id][channel], this.buf));

                this.total_records[this.node_id]  += this.channel_records[this.node_id][channel];
                this.total_bytes[this.node_id]    += this.channel_bytes[this.node_id][channel];
            }

            Trace.formatln("");
         }

        this.printBoxLine();
    }

    /***************************************************************************

        Prints the total items and size of all channels for a paticular node.

    ***************************************************************************/

    private void printNodeTotal () 
    {
        Trace.format("{,21} |", "Total");

        if (this.total_records.length)
        {
            
            foreach (node; this.rowNodeItems)
            {
                this.node_id = node.Address ~ ":" ~ Integer.toString(node.Port);
                if (!(this.node_id in this.errors))
                {
                    Trace.format(" | {,11} |",     formatCommaNumber(this.total_records[this.node_id], this.buf));
                    Trace.format(" {,14} bytes |", formatCommaNumber(this.total_bytes[this.node_id], this.buf));
                }
                else
                {
                    Trace.format(" |{,13}   {,14}      |","","");
                }
            }
        }

        Trace.formatln("");
        this.printBoxLine();
    }

    /***************************************************************************

        Prints the Node info.

    ***************************************************************************/

    private void printNodeInfo ()
    {
        uint i = 0;

        foreach (node; this.rowNodeItems)
        {
            i++;

            if (i==1) 
            {
                Trace.format("{,23} |", "");
                Trace.format("{,35} |", node.Address ~ ":" ~ Integer.toString(node.Port));
            } 
            else 
            {
                Trace.format(" |"); 
                Trace.format("{,35} |", node.Address ~ ":" ~ Integer.toString(node.Port));
            }
        }
        
        Trace.formatln("");
    }

    /***************************************************************************

        Prints the Node info headers - Items,Size.

    ***************************************************************************/

    private void printNodeInfoHeaders ()
    {
        uint i = 0;

        foreach (node; this.rowNodeItems)
        {
            i++;

            if (i==1) 
            {
                Trace.format("{,21} |", "Channel");
                Trace.format(" | {,11} |  {,19} |", "Items", "Size");
            }
            else
            {
                Trace.format(" | {,11} |  {,19} |", "Items", "Size");
            }

         }

        Trace.formatln("");
        this.printBoxLine();
    }

    /***************************************************************************

        Prints the Node Range.

     **************************************************************************/

    private void printNodeRange ()
    {
        uint i = 0;

        foreach (node; this.rowNodeItems)
        {
            i++;
            
            if (i==1) 
            {
                Trace.format("{,23} |", "");
                Trace.format("{,24:X8} - {:X8} |", node.MinValue, node.MaxValue);
            } 
            else 
            {
                Trace.format(" |"); 
                Trace.format("{,24:X8} - {:X8} |", node.MinValue, node.MaxValue);
            }
        }

        Trace.formatln("");
        this.printBoxLine();
    }
    
    
    
    /***************************************************************************
    
        Prints the error associated with the node id.
        
        Param:
        node_id = address:port of node.

     **************************************************************************/
 
    private void printError ( char[] node_id )
    {
        if (node_id in this.errors)
        {
            Trace.format(" | {,30}     |", this.errors[node_id]);
        }
    }
    

    /***************************************************************************

        then the actual number of dht nodes available.

        Returns:
            the number of columns to display from the main configuration.

     **************************************************************************/

    private size_t getDisplayColumns ()
    {
        auto columns = MonitorConfig.columns;
        
        if (this.dhtclient.nodeRegistry.length < columns)
        {
            columns = this.dhtclient.nodeRegistry.length;
        }
        return columns;
    }

    /***************************************************************************

        Prints a horizontal line

     **************************************************************************/

    private void printHeadLine ( size_t columns )
    {
        Trace.format("-----------------------");

        for (uint i=0; i < columns; i++)
        {
            Trace.format("---------------------------------------");
        }
        Trace.formatln("");
    }

    /***************************************************************************

        Prints a horizontal line with spaces.
        
        Params:
            start = 

     **************************************************************************/

    private void printBoxLine ( bool start = true )
    {
        if (start)
        {
            Trace.format("-----------------------");
        }
        else
        {
            Trace.format("                       ");
        }

        foreach (node; this.rowNodeItems)
        {
            Trace.format(" --------------------------------------");
        }

        Trace.formatln("");
    }

    /***************************************************************************

        Creates the list of channels in the DHT node. The method is
        called by AsyncDhtClient.getChannel.

        Param:
            channel = name of the channel

     **************************************************************************/

    private void addChannels ( uint id, char[] channel )
    {
        if ( channel.length && !this.channels.contains(channel) )
        {
            this.channels.length = this.channels.length + 1;
            this.channels[$-1].copy(channel);
        }
    }

    /***************************************************************************

        Retrieves information about the node. This method is called by
        AsyncDhtClient.getChannelSize
        
        Params:
            address = node IP address
            port = node port
            channel = node channel name 
            records = number of records
            bytes = number of bytes
            
     **************************************************************************/
    
    private void addChannelSize ( uint id, char[] address, ushort port, char[] channel, 
            ulong records, ulong bytes )
    {
        if ( channel.length )
        {
            this.node_id = address ~ ":" ~ Integer.toString(port);
            
            this.channel_bytes[this.node_id][channel] = bytes;
            this.channel_records[this.node_id][channel] = records;
        }
    }

    /***************************************************************************

        Formats a number to a string, with comma separation every 3 digits

        TODO: move to tango tango.text.convert.Layout?

     **************************************************************************/

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

