/*******************************************************************************

    DHT node dump
    
    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        October 2010: Initial release
    
    authors:        Gavin Norman
    
    Reads records from one or more dht nodes and outputs them to stdout.
    
    Command line parameters:
        -h = display help
        -s = start of range to query (hash value - defaults to 0x00000000)
        -e = end of range to query   (hash value - defaults to 0xFFFFFFFF)
        -c = channel name to query
        -A = query all channels

*******************************************************************************/

module src.mod.dump.DhtDump;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.dht.DhtClient,
           swarm.dht.DhtHash,
           swarm.dht.DhtConst;

private import swarm.dht.client.DhtNodesConfig;

private import swarm.dht.client.connection.ErrorInfo;

private import tango.io.Stdout;

private import ocean.text.Arguments;


struct DhtDump
{
static:

    /***************************************************************************

        Runs the dht node dump
        
        Params:
            args = command line arguments

    ***************************************************************************/

    public bool run ( Arguments args )
    {
        auto range_min = args.getInt!(hash_t)("start");
        auto range_max = args.getInt!(hash_t)("end");
        auto channel = args.getString("channel");
        auto all_channels = args.getBool("all_channels");

        scope dht = initDhtClient();

        if ( all_channels )
        {
            dumpAllChannels(dht, range_min, range_max);
        }
        else
        {
            dumpChannel(dht, channel, range_min, range_max);
        }

        return true;
    }


    /***************************************************************************

        Initialises a dht client, adding nodes from a config file in
        etc/dhtnodes.xml, and querying the node ranges.

        Returns:
            dht node
    
        Throws:
            asserts that no errors occurred during initialisation

    ***************************************************************************/

    private DhtClient initDhtClient ( )
    {
        bool error;
        auto dht = new DhtClient();
        dht.error_callback(
            (ErrorInfo e)
            {
                Stderr.format("Error info: {}\n", e.message);
                error = true;
            }
        );
        
        DhtNodesConfig.addNodesToClient(dht, "etc/dhtnodes.xml");
        dht.queryNodeRanges().eventLoop();
        assert(!error);

        return dht;
    }


    /***************************************************************************

        Outputs dht records in the specified hash range in all channels to
        stdout. The channels are iterated in series, so all records for the
        first channel will be displayed, followed by all records in the second
        channel, and so on.
        
        Params:
            dht = dht client to perform query with
            range_min = start of hash range to query
            range_max = end of hash range to query
    
    ***************************************************************************/

    private void dumpAllChannels ( DhtClient dht, hash_t range_min, hash_t range_max )
    {
        char[][] channels;
        dht.getChannels(
                ( uint id, char[] channel )
                {
                    channels ~= channel;
                }
            ).eventLoop();

        foreach ( channel; channels )
        {
            dumpChannel(dht, channel, range_min, range_max);
        }
    }


    /***************************************************************************

        Outputs dht records in the specified channel & hash range to stdout.
        
        Params:
            dht = dht client to perform query with
            channel = channel to query
            range_min = start of hash range to query
            range_max = end of hash range to query
    
    ***************************************************************************/
    
    private void dumpChannel ( DhtClient dht, char[] channel, hash_t range_min, hash_t range_max )
    {
        dht.getRange(channel, range_min, range_max,
                ( uint id, char[] key, char[] value )
                {
                    if ( key.length )
                    {
                        Stdout.format("{}: {} -> {}\n", channel, key, value);
                    }
                }
            ).eventLoop();
    }

}

