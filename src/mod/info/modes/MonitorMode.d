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

/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.info.modes.model.IMode;

private import src.mod.info.NodeInfo;

private import src.mod.info.Tables;

private import tango.core.Array : contains;

private import Integer = tango.text.convert.Integer;


private import swarm.dht.DhtClient;


private import ocean.core.Array : appendCopy;

private import ocean.io.Stdout;

private import ocean.text.convert.Layout;


public class MonitorMode : IMode
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


    /***************************************************************************

        The construcor calls its super and sets up the values that are specific
        to this mode.

        Params:
            dht = The dht client that the mode will use.

            dht_id = The name of the DHT that this class is handling. The name
                is used in printin information.

            error_calback = The callback that the display-mode will call to
                pass to it the error messages that it has.

            num_columns = The number of columns that the table will have.

            metric = For large numbers, a short metric unit is written beside
                the number.

    ***************************************************************************/

    public this (DhtClient dht, char[] dht_id,
                IMode.ErrorCallback error_callback,
                int num_columns, bool metric)
    in
    {
        assert(num_columns > 0, "Cannot display 0 columns wide!");
    }
    body
    {
            super(dht, dht_id, error_callback);

            this.table = new Table();
            this.num_columns = num_columns;
            this.metric = metric;
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
            channel_names.length = 0;
            longest_channel_name = 0;
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

        Display the output.

        Params:
           longest_node_name = The size of the longest node name in all DHTs.

    ***************************************************************************/

    public void display ( size_t longest_node_name )
    {
        NodeInfo*[][] node_chunks;

        this.nodes.sort;

        size_t consumed;
        do
        {
            node_chunks.length = node_chunks.length + 1;
            for ( size_t i; i < num_columns; i++ )
            {
                if ( consumed + i < this.nodes.length )
                {
                    node_chunks[$-1] ~= &this.nodes[consumed + i];
                }
            }
            consumed += num_columns;
        }
        while ( consumed < this.nodes.length );

        foreach ( chunk; node_chunks )
        {
            displayNodeChunk(chunk);
        }
    }


    /***************************************************************************

        Prints a subset of the final table output.

        Params:
            nodes = The subset of nodes to print information for.

    ***************************************************************************/

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


    /***************************************************************************

        Add a row to the table.

        Params:
            nodes       = The nodes to be added to the row
            table       = The table that the row will be added to
            first_cell  = The info to go into the first cell.
            node_dg     = The delegate to call for each node.

    ***************************************************************************/

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


