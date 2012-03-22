/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        March 2012: Initial release

    authors:        Gavin Norman, Hatem Oraby


    This displa-mode prints many attributes about a given DHT. For each node
    in the DHT, the class prints the node's hash range. Also for each channel
    that exists in the DHT, the class prints the amount of data that exists
    for that chaneel in for given node.
    The output is printed in a table of w columns. The number of w columns can
    be specified by passing that parameter to the class's constructor.
    The -M flag can be used to print "Megabytes, Gigabyte ..." instead of just
    bytes.

*******************************************************************************/





module src.mod.info.modes.MonitorMode;



private import tango.core.Array : contains;

private import Integer = tango.text.convert.Integer;



private import ocean.core.Array : appendCopy;

private import ocean.io.Stdout;

private import ocean.text.convert.Layout;


private import swarm.dht.DhtClient;



private import src.mod.info.modes.model.IMode;

private import src.mod.info.NodeInfo;

private import src.mod.info.Tables;




class MonitorMode : IMode
{
   /***************************************************************************

    Signals whether the internnal state of whether we need another iteration.

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

    Used to draw a table.

    ***************************************************************************/

    private Table table;

    /***************************************************************************

        Number of columns for monitor display.

    ***************************************************************************/

    private int num_columns;


    /***************************************************************************

        Monitor metric / normal integer display toggle.

    ***************************************************************************/

    private bool metric;



    public this (DhtWrapper wrapper,
              DhtClient.RequestNotification.Callback notifier,
              int num_columns, bool metric)
    in
    {
        assert(num_columns > 0, "Cannot display 0 columns wide!");
    }
    body
    {
            super(wrapper, notifier);

            this.table = new Table();
            this.num_columns = num_columns;
            this.metric = metric;

    }


    public bool run ()
    {

        if (reapeat == false)
        {
            channel_names.length = 0;
            longest_channel_name = 0;
            this.wrapper.dht.assign(this.wrapper.dht.getChannels(
                    &this.channelNamesCallback, this.notifier));

            this.reapeat = true;
        }
        else
        {
            this.channel_names.sort;

            // Get channel size info
            foreach ( channel; this.channel_names )
            {
                this.wrapper.dht.assign(
                    this.wrapper.dht.getChannelSize(channel,
                                                   &this.channelSizeCallback,
                                                   this.notifier));
            }
            this.reapeat = false;
        }



        return this.reapeat;
    }





    void channelNamesCallback ( DhtClient.RequestContext context,
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



    private void channelSizeCallback ( DhtClient.RequestContext context,
                                char[] address, ushort port, char[] channel,
                                ulong records, ulong bytes )
    {
        auto node = this.wrapper.findNode(address, port);
        if ( !node )
        {
            Stderr.formatln("Node mismatch");
        }
        else
        {
            node.setChannelSize(channel, records, bytes);
        }

    }

    public void display ( size_t longest_node_name )
    {
        NodeInfo*[][] node_chunks;

        this.wrapper.nodes.sort;

        size_t consumed;
        do
        {
            node_chunks.length = node_chunks.length + 1;
            for ( size_t i; i < num_columns; i++ )
            {
                if ( consumed + i < this.wrapper.nodes.length )
                {
                    node_chunks[$-1] ~= &this.wrapper.nodes[consumed + i];
                }
            }
            consumed += num_columns;
        }
        while ( consumed < this.wrapper.nodes.length );

        foreach ( chunk; node_chunks )
        {
            displayNodeChunk(chunk);
        }
    }


    private void displayNodeChunk ( NodeInfo*[] nodes )
    {
        char[] tmp;


        version (X86_64)
        {
            const hash_format = "0x{:x16}";
        }
        else
        {
            const hash_format = "0x{:x8}";
        }

        this.table.init(1 + (nodes.length * 2));

        // Node addresses / ports
        this.table.firstRow.setDivider(1);

        this.addRow(nodes, this.table, Table.Cell.Empty(),
                ( NodeInfo* node, ref Table.Cell cell1, ref Table.Cell cell2 )
                {
                    cell1.setMerged;
                    cell2.setString(node.address ~
                                            ":" ~ Integer.toString(node.port));
                });

        // Node hash ranges
        addRow(nodes, this.table, Table.Cell.Empty(),
                ( NodeInfo* node, ref Table.Cell cell1, ref Table.Cell cell2 )
                {
                    tmp.length = 0;
                    Layout!(char).print(tmp, "Min: " ~ hash_format,
                                        node.min_hash);
                    cell1.setMerged;
                    cell2.setString(tmp);
                });

        // Node hash ranges
        addRow(nodes, this.table, Table.Cell.Empty(),
                ( NodeInfo* node, ref Table.Cell cell1, ref Table.Cell cell2 )
                {
                    tmp.length = 0;
                    Layout!(char).print(tmp, "Max: " ~ hash_format,
                                        node.max_hash);
                    cell1.setMerged;
                    cell2.setString(tmp);
                });


        this.table.nextRow.setDivider();

        // Column headers
        addRow(nodes, this.table, Table.Cell.String("Channel"),
                ( NodeInfo* node, ref Table.Cell cell1, ref Table.Cell cell2 )
                {
                    cell1.setString("Records");
                    cell2.setString("Bytes");
                });

        this.table.nextRow.setDivider();

        // Channel contents
        foreach ( channel; this.channel_names )
        {
            addRow(nodes, this.table, Table.Cell.String(channel),
                  ( NodeInfo* node, ref Table.Cell cell1, ref Table.Cell cell2 )
                  {
                        ulong items, size;
                        bool queried;
                        node.getChannelSize(channel, items, size, queried);

                        if ( queried )
                        {
                            if ( this.metric )
                            {
                                cell1.setDecimalMetric(items);
                                cell2.setBinaryMetric(size, "B");
                            }
                            else
                            {
                                cell1.setInteger(items);
                                cell2.setInteger(size);
                            }
                        }
                        else
                        {
                            cell1.setMerged;
                            cell2.setString("CONNECTION ERROR");
                        }
                    });
        }

        this.table.nextRow.setDivider();

        // Totals
        addRow(nodes, this.table, Table.Cell.String("Total"),
                ( NodeInfo* node, ref Table.Cell cell1, ref Table.Cell cell2 )
                {
                    ulong items, size;
                    if ( node.range_queried )
                    {
                        foreach ( channel; node.channels )
                        {
                            items += channel.records;
                            size += channel.bytes;
                        }
                    }

                    if ( this.metric )
                    {
                        cell1.setDecimalMetric(items);
                        cell2.setBinaryMetric(size, "B");
                    }
                    else
                    {
                        cell1.setInteger(items);
                        cell2.setInteger(size);
                    }
                });

        this.table.nextRow.setDivider();

        this.table.display();

        Stdout.formatln("");
    }


    private void addRow ( NodeInfo*[] nodes, Table table, Table.Cell first_cell,
            void delegate ( NodeInfo* node, ref Table.Cell c1,
            ref Table.Cell c2 ) node_dg )
    {
        auto row = table.nextRow;

        row.cells[0] = first_cell;

        foreach ( i, ref node; nodes )
        {
            node_dg(node, row.cells[(i * 2) + 1], row.cells[(i * 2) + 2]);
        }
    }
}


