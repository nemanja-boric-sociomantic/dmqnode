/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        March 2012: Initial release

    authors:        Gavin Norman, Hatem Oraby


    This display-mode prints the hash range of each node in a given DHT.

*******************************************************************************/




module src.mod.info.modes.HashRangesMode;



private import ocean.io.Stdout;


private import swarm.dht.DhtClient;


private import src.mod.info.modes.model.IMode;



class HashRangesMode : IMode
{

    public this (DhtWrapper wrapper,
              DhtClient.RequestNotification.Callback notifier)
    {
            super(wrapper, notifier);
    }


    public bool run ()
    {
        return false;
    }


    /***************************************************************************

        Display the output.

    ***************************************************************************/

    public void display (size_t longest_node_name )
    {
        Stdout.formatln("\nHash ranges:");
        Stdout.formatln("------------------------------------------------------------------------------");

        this.wrapper.nodes.sort;
        foreach ( i, node; this.wrapper.nodes )
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

