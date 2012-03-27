/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        March 2012: Initial release

    authors:        Gavin Norman, Hatem Oraby


    The display-mode prints the number of connections per each node in a
    given DHT. If one of the nodes didn't respond, then this will be printed.

*******************************************************************************/

module src.mod.info.modes.NumOfConnectionsMode;

/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.info.modes.model.IMode;


private import swarm.dht.DhtClient;


private import ocean.io.Stdout;

private import ocean.text.util.DigitGrouping;


class NumOfConnectionsMode : IMode
{

    public this (DhtClient dht, char[] dht_id,
                IMode.ErrorCallback error_callback)
    {
            super(dht, dht_id, error_callback);
    }


    public bool run ()
    {
        foreach (ref node; this.nodes)
        {
            node.responded = false;
        }

        // Query all nodes for their active connections
        this.dht.assign(this.dht.getNumConnections(
                &this.callback, &this.local_notifier));

        return false;
    }


    void callback ( DhtClient.RequestContext context, char[] node_address,
                                ushort node_port, size_t num_connections )
    {
        auto node = this.findNode(node_address, node_port);
        if ( !node )
        {
            Stderr.formatln("Node mismatch");
        }
        else
        {
            node.connections = num_connections;
        }
    }


    /***************************************************************************

        Display the output.

    ***************************************************************************/

    public void display (size_t longest_node_name )
    {
        this.nodes.sort;
        foreach ( i, node; this.nodes )
        {
            char[] node_name;
            node.name(node_name);
            bool node_queried = node.connections < size_t.max;
            this.outputConnectionsRow(i, node_name, longest_node_name,
                                    node_queried, node.connections - 1);
        }
    }


    /***************************************************************************

        Outputs a connections info row to Stdout.

        Params:
            num = number to prepend to row
            name = name of row item
            longest_name = length of the longest string of type name, used to
                work out how wide the name column needs to be
            node_queried = true if node connections were successfully queried
            connections = number of connections

    ***************************************************************************/

    private void outputConnectionsRow ( uint num, char[] name,
            size_t longest_name, bool node_queried, uint connections )
    {
        char[] pad;
        pad.length = longest_name - name.length;
        pad[] = ' ';

        if ( node_queried )
        {
            char[] connections_str;
            DigitGrouping.format(connections, connections_str);

            Stdout.formatln("  {,3}: {}{} {,5} connections",
                            num, name, pad, connections_str);
        }
        else
        {
            Stdout.formatln("  {,3}: {}{} <node did not respond>",
                                num, name, pad);
        }
    }
}

