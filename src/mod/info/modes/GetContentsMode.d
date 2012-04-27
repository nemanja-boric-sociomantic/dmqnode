/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        March 2012: Initial release

    authors:        Gavin Norman, Hatem Oraby


    The GetContentsMode display-mode prints the content of each node in a given
    DHT. The content it outputs is:
    - For each channel, it displays the channel name..
    - The size that each channel occupies in each node.
    If the verbose flag is used, the more detailed information is printed.

*******************************************************************************/

module src.mod.info.modes.GetContentsMode;

/*******************************************************************************

    Imports

*******************************************************************************/


private import src.mod.info.modes.model.IMode;


private import swarm.dht.DhtClient;


private import ocean.io.Stdout;

private import ocean.core.Array : appendCopy;

private import ocean.text.util.DigitGrouping;


private import tango.core.Array : contains;

private import Integer = tango.text.convert.Integer;


public class GetContentsMode : IMode
{
    /***************************************************************************

        Signals whether the internnal state of whether we need another iteration

    ***************************************************************************/

    private bool reapeat = false;

    /***************************************************************************

        Used for print formatting purposes.

    ***************************************************************************/

    private size_t longest_channel_name;

    /***************************************************************************

        Holds the names of the DHT channel names.

    ***************************************************************************/

    private char[][] channel_names;

    /***************************************************************************

        Flags whether the verbose mode should be used.

    ***************************************************************************/

    private bool verbose;


    /***************************************************************************

        The construcor just calls its super.

        Params:
            dht = The dht client that the mode will use.

            dht_id = The name of the DHT that this class is handling. The name
                is used in printin information.

            error_calback = The callback that the display-mode will call to
                pass to it the error messages that it has.

    ***************************************************************************/

    public this (DhtClient dht, char[] dht_id,
              IMode.ErrorCallback error_callback,
              bool verbose = false)
    {
            super(dht, dht_id, error_callback);
            this.verbose = verbose;
    }


    /***************************************************************************

        The method called for each DHT.

        The method just assigns the callbacks that will be run when the event
        loop is later (externally) called.

        Returns:
            Returns true if the method has still more dht assign to be assigned
            but which needs to be assigned on multiple event-loop calls. This
            is done as to perform the second dht assign tasks, the results of
            the first dht assigns are needed.
            Returns false if all the dht tasks had been already carried out.

    ***************************************************************************/

    public bool run ()
    {
        foreach (ref node; this.nodes)
        {
            node.responded = false;
        }

        if (reapeat == false)
        {
            this.channel_names.length = 0;
            this.longest_channel_name = 0;
            this.dht.assign(this.dht.getChannels( &this.channelNamesCallback,
                                                &this.local_notifier));

            this.reapeat = true;
        }
        else
        {
            this.channel_names.sort;

            // Get channel size info
            foreach ( channel; this.channel_names )
            {
                this.dht.assign( this.dht.getChannelSize(channel,
                                 &this.channelSizeCallback,
                                 &this.local_notifier));
            }
            this.reapeat = false;
        }

        return this.reapeat;
    }


    /***************************************************************************

        The callback stores the retrieved channel name in a list and "remembers"
        the length of the longest channel.

        Params:
            context = Call context (ignored).
            address = The address of the replying node.
            port    = The port of the replying node.
            channel = The channel nam.

    ***************************************************************************/

    private void channelNamesCallback ( DhtClient.RequestContext context,
                    char[] address, ushort port, char[] channel )
    {
        if ( channel.length && !this.channel_names.contains(channel) )
        {
            this.channel_names.appendCopy(channel);
            if ( channel.length > this.longest_channel_name )
            {
                this.longest_channel_name = channel.length;
            }
        }
    }


    /***************************************************************************

        The callback stores the channel size for each retrieved channel.

        Params:
            context = Call context (ignored).
            address = The address of the replying node.
            port    = The port of the replying node.
            channel = The channel name for which the size is reported.
            records = Numbe of records in that channel.
            byte    = Numbe of bytes stored in that channel in this node.

    ***************************************************************************/

    private void channelSizeCallback ( DhtClient.RequestContext context,
                                char[] address, ushort port, char[] channel,
                                ulong records, ulong bytes )
    {
        auto node = this.findNode(address, port);
        if ( !node )
        {
            Stderr.formatln("Node mismatch");
        }
        else
        {
            node.setChannelSize(channel, records, bytes);
        }

    }


    /***************************************************************************

        Queries and displays the size of the contents of each channel and node.

        Params:
            longest_node_name = the length of the longest node name string

    ***************************************************************************/

    public void display ( size_t longest_node_name )
    {

        this.nodes.sort;
        Stdout.flush();

        // Display channels
        Stdout.formatln("\nChannels:");
        Stdout.formatln("------------------------------------------------------------------------------");

        if ( this.verbose )
        {
            foreach ( i, channel; this.channel_names )
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

                    this.outputSizeRow(j, node_name, longest_node_name,
                                        node_queried, records, bytes);
                }

                this.outputSizeTotal(longest_node_name, channel_records,
                                    channel_bytes);
            }
        }
        else
        {
            foreach ( i, channel; this.channel_names )
            {
                ulong records, bytes;
                foreach ( node; this.nodes )
                {
                    ulong channel_records, channel_bytes;
                    bool node_queried;
                    node.getChannelSize(channel, channel_records,
                            channel_bytes, node_queried);
                    records += channel_records;
                    bytes += channel_bytes;
                }

                this.outputSizeRow(i, channel, longest_channel_name,
                                    true, records, bytes);
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
                        this.outputSizeRow(j, ch.name, longest_channel_name,
                                            node_queried, ch.records, ch.bytes);
                        node_records += ch.records;
                        node_bytes += ch.bytes;
                    }
                }
                else
                {
                    this.outputSizeRow(0, "", longest_channel_name,
                                        node_queried, 0, 0);
                }

                this.outputSizeTotal(longest_channel_name, node_records,
                                        node_bytes);
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

                char[] node_name = node.address ~
                                    ":" ~ Integer.toString(node.port);

                this.outputSizeRow(i, node_name, longest_node_name,
                                        node_queried, records, bytes);
            }
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

    private void outputSizeRow ( uint num, char[] name, size_t longest_name,
                                bool node_queried, ulong records, ulong bytes )
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

            Stdout.formatln("  {,3}: {}{} {,17} records {,17} bytes",
                            num,name, pad, records_str, bytes_str);
        }
        else
        {
            Stdout.formatln("  {,3}: {}{}    <node did not respond>",
                            num, name, pad);
        }
    }


    /***************************************************************************
    
        Outputs a sum row to Stdout.

        Params:
            records = number of records
            bytes = number of bytes

    ***************************************************************************/

    private void outputSizeTotal ( size_t longest_name, ulong records,
                                    ulong bytes )
    {
        char[] pad;
        pad.length = longest_name;
        pad[] = ' ';

        char[] records_str;
        DigitGrouping.format(records, records_str);

        char[] bytes_str;
        DigitGrouping.format(bytes, bytes_str);

        Stdout.formatln("Total: {} {,17} records {,17} bytes",
                        pad, records_str, bytes_str);
    }

}




