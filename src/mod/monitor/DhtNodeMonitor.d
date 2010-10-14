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

private import  core.config.MainConfig;

private import  swarm.dht.DhtClient;

private import  swarm.dht.DhtHash, swarm.dht.DhtConst;

private import  tango.core.Thread;

private import  tango.core.Array;

private import  tango.time.Clock;

private import  tango.util.Arguments;

private import  tango.util.log.Trace;

private import  Integer = tango.text.convert.Integer;



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

class NodeMonDaemon : DhtClient
{
    /***************************************************************************

        DHT configuration file 

     **************************************************************************/

    const char[][] DhtNodeCfg     = ["etc", "dhtnodes.xml"];

    /***************************************************************************

        Number of seconds between display updates

     **************************************************************************/

    static public const WAIT_TIME = 10;

    /***************************************************************************

        List of channels in DHT node - created by constructor

     **************************************************************************/

    protected char[][] channels;
    
    /***************************************************************************

        Array of error messages, node ip and port are key.
    
     **************************************************************************/
    protected char[][ char[] ] errors;

    /***************************************************************************

        Minimum value for responsible range

     **************************************************************************/

    protected char[] range_min;

    /***************************************************************************

        Maximum value for responsible range

     **************************************************************************/

    protected char[] range_max;

    /***************************************************************************

        Number of bytes for a channel

     **************************************************************************/

    protected ulong c_bytes[char[]][char[]];

    /***************************************************************************

        Number of records for a channel

     **************************************************************************/

    protected ulong c_records[char[]][char[]];

    /***************************************************************************

        Total number of bytes for all channels

     **************************************************************************/

    protected ulong t_bytes[char[]];

    /***************************************************************************

        Total number of records for all channels

     **************************************************************************/

    protected ulong t_records[char[]];

    /***************************************************************************

        Buffer for node id

     **************************************************************************/

    protected char[] node_id;

    /***************************************************************************

        Buffer for thousand separator method

        TODO

     **************************************************************************/

    protected char[] buf;

    /***************************************************************************

        NodeItems for display method

     **************************************************************************/

    protected DhtConst.NodeItem[]   nodeItems;

    /***************************************************************************

        Number of columns to display

     **************************************************************************/

    protected uint  display_cols;

    /***************************************************************************

        Constructor

     **************************************************************************/

    public this ()
    {
        hash_t range_min, range_max;
        DhtHash.HexDigest hash;

        new Thread(&this.run);

        foreach (node; MainConfig.getDhtNodeItems())
        {
            this.addNode(node.Address, node.Port);
        }
        
        super.queryNodeRanges().eventLoop();

        super.error_callback = &this.onConnectionError;

    }
    
    
    
    /***************************************************************************
    
        Receives error information from the DhtClient
    
        Returns:
            void
    
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

        Returns:
            void

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

        Returns:
            void

    ***************************************************************************/

    protected void update ()
    {
        
        foreach (k;this.errors.keys)this.errors.remove(k);
        foreach (k;this.c_bytes.keys)this.c_bytes.remove(k);
        foreach (k;this.t_bytes.keys)this.t_bytes.remove(k);
        foreach (k;this.c_records.keys)this.c_records.remove(k);
        foreach (k;this.t_records.keys)this.t_records.remove(k);
        
        this.getChannels(&this.addChannels);
        this.eventLoop();

        foreach (channel; this.channels)
        {
            this.getChannelSize(channel, &this.addChannelSize);
            this.eventLoop();
        }

        this.print();
    }

    /***************************************************************************

        Prints all fetched data to Stdout.

        Returns:
            void

    ***************************************************************************/

    private void print ()
    {
        uint i,t    = 0;

        this.getDisplayColumns();

        this.printTime();

        foreach (node; super)
        {

            i++, t++;
            this.nodeItems ~= node.nodeitem;

            if ((i==this.display_cols) || 
                ( (MainConfig.getDhtNodeItems().length-t)+i < this.display_cols))
            {
                Trace.formatln("");
                i=0;
            }
            else
            {
                continue;
            }

            this.printRow();
            this.nodeItems.length=0;
        }
    }

    /***************************************************************************

        Prints one row of data. Row length is determined by the 
        "Monitor : display_cols" configuration setting.

        Returns:
            void

    ***************************************************************************/

    private void printRow ()
    {
       this.printBoxLine(false);

       this.printNodeInfo();

       this.printNodeRange();

       this.printNodeInfoHeaders();

       this.printNodeChannels();

       this.printNodeTotal();
    }

    /***************************************************************************

        Prints the current time and number of nodes.

        Returns:
            void

    ***************************************************************************/

    private void printTime ()
    {
       this.printHeadLine();

        Trace.formatln(" Time: {}            Number of Nodes: {}",
                Clock.now(), MainConfig.getDhtNodeItems().length);

        this.printHeadLine();
    }

    /***************************************************************************

        Prints a list of channels.

        Returns:
            void

    ***************************************************************************/

    private void printNodeChannels () 
    {

        foreach (channel; this.channels)
        {

            Trace.format("{,21} |", channel);

            foreach (node; this.nodeItems)
            {
                try
                {
                    this.node_id = node.Address ~ ":" ~ Integer.toString(node.Port);
                    
                    this.printError(this.node_id);
                    
                    Trace.format(" | {,11} | {,13} bytes  |",
                            typeof(this).formatCommaNumber(this.c_records[this.node_id][channel], this.buf),
                            typeof(this).formatCommaNumber(this.c_bytes[this.node_id][channel], this.buf));
                    this.t_records[this.node_id]  += this.c_records[this.node_id][channel];
                    this.t_bytes[this.node_id]    += this.c_bytes[this.node_id][channel];
                } 
                catch ( Exception e )
                {
                }
            }

            Trace.formatln("");
            this.printBoxLine();
         }

        this.printBoxLine();
    }

    /***************************************************************************

        Prints the total items and size of all channels for a paticular node.

        Returns:
            void

    ***************************************************************************/

    private void printNodeTotal () 
    {
        Trace.format("{,21} |", "Total");

        if (this.t_records.length)
        {
            
            foreach (node; this.nodeItems)
            {
                this.node_id = node.Address ~ ":" ~ Integer.toString(node.Port);
                if (!(this.node_id in this.errors))
                {
                    try
                    {
                        Trace.format(" |{,13}|{,14} bytes  |",
                        typeof(this).formatCommaNumber(this.t_records[this.node_id], this.buf), 
                        typeof(this).formatCommaNumber(this.t_bytes[this.node_id], this.buf));
                    } 
                    catch ( Exception e)
                    {
                    }
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

        Returns:
            void

    ***************************************************************************/

    private void printNodeInfo ()
    {
        uint i = 0;

        foreach (node; this.nodeItems)
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

        Returns:
            void

    ***************************************************************************/

    private void printNodeInfoHeaders ()
    {
        uint i = 0;

        foreach (node; this.nodeItems)
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

        Returns:
            void

    ***************************************************************************/

    private void printNodeRange ()
    {
        uint i = 0;

        foreach (node; this.nodeItems)
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

        Returns:
            void
    
    ***************************************************************************/
 
    private void printError ( char[] node_id )
    {
        if (node_id in this.errors)
        {
            Trace.format(" | {,30}     |", this.errors[node_id]);
        }
    }
    

    /***************************************************************************

        Gets the number of columns to display from the main configuration.

        Returns:
            void

    ***************************************************************************/

    private void getDisplayColumns ()
    {
        this.display_cols = Config.get!(uint)("Monitor", "display_cols");

        if (this.display_cols>=MainConfig.getDhtNodeItems().length) 
        {
            this.display_cols = MainConfig.getDhtNodeItems().length;
        }
    }

    /***************************************************************************

        Prints a horizontal line

        Returns:
            void

     ***************************************************************************/

    private void printHeadLine ()
    {
        Trace.format("-----------------------");

        for (uint i=0; i<this.display_cols; i++)
        {
            Trace.format("---------------------------------------");
        }
        Trace.formatln("");
    }

    /***************************************************************************

        Prints a horizontal line

        Returns:
            void

    ***************************************************************************/

    private void printLine ()
    {
        Trace.format("-----------------------");

        foreach (node; MainConfig.getDhtNodeItems())
        {
            Trace.format("---------------------------------------");
        }
        Trace.formatln("");
    }

    /***************************************************************************

        Prints a horizontal line with spaces.

        Returns:
            void

    ***************************************************************************/

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

        foreach (node; this.nodeItems)
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

        Returns:
            void

     **************************************************************************/

    private void addChannels ( uint id, char[] channel )
    {
        if ( channel.length && !this.channels.contains(channel) )
        {
            this.channels ~= channel.dup;
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
            
        Returns:
            void            

     **************************************************************************/
    
    private void addChannelSize ( uint id, char[] address, ushort port, char[] channel, 
            ulong records, ulong bytes )
    {
        if ( channel.length )
        {
            this.node_id = address ~ ":" ~ Integer.toString(port);
            
            this.c_bytes[this.node_id][channel] = bytes;
            this.c_records[this.node_id][channel] = records;
        }
    }

    /***************************************************************************

        Formats a number to a string, with comma separation every 3 digits

        TODO: Should be moved to tango tango.text.convert.Layout

     **************************************************************************/

    private static char[] formatCommaNumber ( T ) ( T num, out char[] str )
    {
        auto string = Integer.toString(num);

        bool comma;
        size_t left = 0;
        size_t right = left + 3;
        size_t first_comma;

        if ( string.length > 3 )
        {
            comma = true;
            first_comma = string.length % 3;

            if ( first_comma > 0 )
            {
                right = first_comma;
            }
        }

        do
        {
            if ( right >= string.length )
            {
                right = string.length;
                comma = false;
            }
            str ~= string[left..right];
            if ( comma )
            {
                str ~= ",";
            }

            left = right;
            right = left + 3;
        } while( left < string.length );

        return str;
    }
}

