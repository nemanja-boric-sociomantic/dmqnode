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
    private struct Column
    {
        enum Type
        {
            Empty,
            Divider,
            Number,
            String
        }

        Type type;

        // TODO: could be a union
        uint number;
        char[] string;

        size_t width;

        void set ( char[] str, size_t width )
        {
            this.type = Type.String;
            this.string.copy(str);
            this.width = width;
        }

        void set ( uint num, size_t width )
        {
            this.type = Type.Number;
            this.number = num;
            this.width = width;
        }

        void setEmpty ( size_t width )
        {
            this.type = Type.Empty;
            this.width = width;
        }

        void setDivider ( size_t width )
        {
            this.type = Type.Divider;
            this.width = width;
        }
    }


    static public void display ( NodeInfo[] nodes, size_t num_columns, size_t longest_node_name, char[][] channel_names, size_t longest_channel_name )
    in
    {
        assert(num_columns > 0, "Cannot display 0 columns wide!");
    }
    body
    {
        // Ensure minimum node column width
        if ( longest_node_name < 32 )
        {
            longest_node_name = 32;
        }

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
            displayNodeChunk(chunk, longest_node_name, channel_names, longest_channel_name);
        }
    }


    static private void displayNodeChunk ( NodeInfo*[] nodes, size_t longest_node_name, char[][] channel_names, size_t longest_channel_name )
    {
        Column[] row;
        char[] tmp;

        auto num_columns = nodes.length;

        // sub column widths
        auto items_column_width = (longest_node_name - 2) / 2;
        auto size_column_width = (longest_node_name - 1) - items_column_width;

        // Top
        drawDivider(num_columns, longest_channel_name, longest_node_name, true);

        // Node names
        drawRow(num_columns, longest_channel_name, longest_node_name,
                ( ref Column col, int num )
                {
                    nodes[num].name(tmp);
                    col.set(tmp, longest_node_name + 1);
                }, true);

        // Node hash ranges
        drawRow(num_columns, longest_channel_name, longest_node_name,
                ( ref Column col, int num )
                {
                    nodes[num].range(tmp);
                    col.set(tmp, longest_node_name + 1);
                }, true);

        // Divider
        drawDivider(num_columns, longest_channel_name, longest_node_name);

        // Columns headers
        drawSubRow("Channel", num_columns, longest_channel_name, longest_node_name,
                ( ref Column col1, ref Column col2, int num )
                {
                    col1.set("Records", items_column_width);
                    col2.set("Bytes", size_column_width);
                });

        // Divider
        drawDivider(num_columns, longest_channel_name, longest_node_name);

        // Channel contents
        foreach ( channel; channel_names )
        {
            drawSubRow(channel, num_columns, longest_channel_name, longest_node_name,
                    ( ref Column col1, ref Column col2, int num )
                    {
                        ulong items, size;
                        bool queried;
                        nodes[num].getChannelSize(channel, items, size, queried);

                        if ( queried )
                        {
                            col1.set(items, items_column_width);
                            col2.set(size, size_column_width);
                        }
                        else
                        {
                            col1.set("CONNECTION", items_column_width);
                            col2.set("ERROR", size_column_width);
                        }
                    });
        }

        // Divider
        drawDivider(num_columns, longest_channel_name, longest_node_name);

        // Totals
        drawSubRow("Total", num_columns, longest_channel_name, longest_node_name,
                ( ref Column col1, ref Column col2, int num )
                {
                    ulong items, size;
                    if ( nodes[num].range_queried )
                    {
                        foreach ( channel; nodes[num].channels )
                        {
                            items += channel.records;
                            size += channel.bytes;
                        }
                    }

                    col1.set(items, items_column_width);
                    col2.set(size, size_column_width);
                });

        // Bottom
        drawDivider(num_columns, longest_channel_name, longest_node_name);

        Stdout.formatln("");
    }


    static private void drawDivider ( size_t num_columns, size_t longest_channel_name, size_t longest_node_name, bool empty_first = false )
    {
        drawRow(num_columns, longest_channel_name, longest_node_name,
                ( ref Column row, int num )
                {
                    row.setDivider(longest_node_name + 1);
                }, empty_first);
    }


    static private void drawRow ( size_t num_columns, size_t longest_channel_name, size_t longest_node_name,
            void delegate ( ref Column col, int num ) col_dg, bool empty_first = false )
    {
        Column[] row;
        row.length = num_columns + 1;

        if ( empty_first )
        {
            row[0].setEmpty(longest_channel_name);
        }
        else
        {
            row[0].setDivider(longest_channel_name);
        }

        for ( int i; i < num_columns; i++ )
        {
            col_dg(row[i + 1], i);
        }
        drawRow(row);
    }


    static private void drawSubRow ( char[] first_col, size_t num_columns, size_t longest_channel_name, size_t longest_node_name,
            void delegate ( ref Column col1, ref Column col2, int num ) col_dg )
    {
        Column[] row;
        row.length = (num_columns * 2) + 1;

        row[0].set(first_col, longest_channel_name);

        for ( int i; i < num_columns; i++ )
        {
            col_dg(row[1 + (i * 2)], row[2 + (i * 2)], i);
        }
        drawRow(row);
    }


    static private void drawRow ( Column[] row )
    {
        char[] num_as_string;
        char[] padding;

        foreach ( col; row )
        {
            switch ( col.type )
            {
                case col.type.Empty:
                    padding.length = col.width + 1;
                    padding[] = ' ';
                    Stdout.format("{}", padding);
                    break;

                case col.type.Divider:
                    padding.length = col.width + 1;
                    padding[] = '-';
                    Stdout.format("{}", padding);
                    break;

                case col.type.Number:
                    DigitGrouping.format(col.number, num_as_string);
                    padding.length = col.width - num_as_string.length;
                    padding[] = ' ';
                    Stdout.format("{}{} ", padding, num_as_string);
                    break;

                case col.type.String:
                    padding.length = col.width - col.string.length;
                    padding[] = ' ';
                    Stdout.format("{}{} ", padding, col.string);
                    break;
            }
            
            Stdout.format("|");
        }

        Stdout.formatln("");
    }
}

