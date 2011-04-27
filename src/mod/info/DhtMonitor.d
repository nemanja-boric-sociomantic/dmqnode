/*******************************************************************************

    Dht monitor display helper

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.info.DhtMonitor;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.info.Tables;

private import src.mod.info.NodeInfo;

private import ocean.core.Array;

private import ocean.text.util.DigitGrouping;

private import tango.io.Stdout;

private import Integer = tango.text.convert.Integer;



/*******************************************************************************

    Dht monitor display helper -- just a container, do not instantiate.

*******************************************************************************/

public class DhtMonitor
{
    static private Table table;

    static this ( )
    {
        table = new Table;
    }

    static public void display ( NodeInfo[] nodes, size_t num_columns, char[][] channel_names )
    in
    {
        assert(num_columns > 0, "Cannot display 0 columns wide!");
    }
    body
    {
        NodeInfo*[][] node_chunks;
        size_t consumed;
        do
        {
            node_chunks.length = node_chunks.length + 1;
            for ( size_t i; i < num_columns; i++ )
            {
                if ( consumed + i < nodes.length )
                {
                    node_chunks[$-1] ~= &nodes[consumed + i];
                }
            }
            consumed += num_columns;
        }
        while ( consumed < nodes.length );

        foreach ( chunk; node_chunks )
        {
            displayNodeChunk(chunk, channel_names);
        }
    }


    static private void displayNodeChunk ( NodeInfo*[] nodes, char[][] channel_names )
    {
        char[] tmp;

        table.init(1 + (nodes.length * 2));

        // Node addresses / ports
        table.firstRow.setDivider(1);

        addRow(nodes, table, Table.Cell.Empty(), 
                ( NodeInfo* node, ref Table.Cell cell1, ref Table.Cell cell2 )
                {
                    cell1.setMerged;
                    cell2.setString(node.address);
                });

        // Node hash ranges
        addRow(nodes, table, Table.Cell.Empty(),
                ( NodeInfo* node, ref Table.Cell cell1, ref Table.Cell cell2 )
                {
                    node.range(tmp);
                    cell1.setMerged;
                    cell2.setString(tmp);
                });

        table.nextRow.setDivider();

        // Column headers
        addRow(nodes, table, Table.Cell.String("Channel"),
                ( NodeInfo* node, ref Table.Cell cell1, ref Table.Cell cell2 )
                {
                    cell1.setString("Records");
                    cell2.setString("Bytes");
                });

        table.nextRow.setDivider();

        // Channel contents
        foreach ( channel; channel_names )
        {
            addRow(nodes, table, Table.Cell.String(channel),
                    ( NodeInfo* node, ref Table.Cell cell1, ref Table.Cell cell2 )
                    {
                        ulong items, size;
                        bool queried;
                        node.getChannelSize(channel, items, size, queried);

                        if ( queried )
                        {
                            cell1.setInteger(items);
                            cell2.setInteger(size);
                        }
                        else
                        {
                            cell1.setMerged;
                            cell2.setString("CONNECTION ERROR");
                        }
                    });
        }

        table.nextRow.setDivider();

        // Totals
        addRow(nodes, table, Table.Cell.String("Total"),
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

                    cell1.setInteger(items);
                    cell2.setInteger(size);
                });

        table.nextRow.setDivider();

        table.display();

        Stdout.formatln("");
    }


    static private void addRow ( NodeInfo*[] nodes, Table table, Table.Cell first_cell,
            void delegate ( NodeInfo* node, ref Table.Cell c1, ref Table.Cell c2 ) node_dg )
    {
        auto row = table.nextRow;

        row.cells[0] = first_cell;

        foreach ( i, ref node; nodes )
        {
            node_dg(node, row.cells[(i * 2) + 1], row.cells[(i * 2) + 2]);
        }
    }
}

