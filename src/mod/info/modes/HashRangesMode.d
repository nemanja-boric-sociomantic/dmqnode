/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        March 2012: Initial release

    authors:        Gavin Norman, Hatem Oraby


    This display-mode prints the hash range of each node in a given DHT.

*******************************************************************************/

module src.mod.info.modes.HashRangesMode;

/*******************************************************************************

    Imports

*******************************************************************************/


private import src.mod.info.modes.model.IMode;

private import swarm.dht.DhtClient;

private import ocean.io.Stdout;


public class HashRangesMode : IMode
{
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
                IMode.ErrorCallback error_callback)
    {
            super(dht, dht_id, error_callback);
    }


    /***************************************************************************

        The method called for each DHT.

        The method just assigns the callbacks that will be run when the event
        loop is later (externally) called.

        Returns:
            The method always returns false in this class's case.

    ***************************************************************************/

    public bool run ()
    {
        //In contrary to other display-modes classes, We will set here the
        // nodes.responded to true as we didn't send any requests at the
        // first place to wait responses for.
        foreach (ref node; this.nodes)
        {
            node.responded = true;
        }

        return false;
    }


    /***************************************************************************

        Display the output.

        Params:
           longest_node_name = The size of the longest node name in all DHTs.

    ***************************************************************************/

    public void display (size_t longest_node_name )
    {
        Stdout.formatln("\nHash ranges:");
        Stdout.formatln("------------------------------------------------------------------------------");

        this.nodes.sort;
        foreach ( i, node; this.nodes )
        {
            char[] name_str;
            node.name(name_str);
            this.outputHashRangeRow(i, name_str, longest_node_name,
                node.range_queried, node.min_hash, node.max_hash);
        }
    }


    /***************************************************************************
    
        Outputs a hash range info row to Stdout.
    
        Params:
            num = number to prepend to row
            name = name of row item
            longest_name = length of the longest string of type name, used to
                work out how wide the name column needs to be
            range_queried = true if node hash range was successfully queried
            min = min hash
            max = mas hash
    
    ***************************************************************************/

    private void outputHashRangeRow ( uint num, char[] name,
            size_t longest_name, bool range_queried, hash_t min, hash_t max )
    {
        char[] pad;
        pad.length = longest_name - name.length;
        pad[] = ' ';

        if ( range_queried )
        {
            Stdout.formatln("  {,3}: {}{}   0x{:X8} .. 0x{:X8}",
                                        num, name, pad, min, max);
        }
        else
        {
            Stdout.formatln("  {,3}: {}{}   <node did not respond>",
                                                    num, name, pad);
        }
    }
}

