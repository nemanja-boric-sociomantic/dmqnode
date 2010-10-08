/*******************************************************************************

    DHT node dump
    
    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        October 2010: Initial release
    
    authors:        Gavin Norman

    Reads records from a dht node/s and outputs them to stdout.

    Command line params:
        -s = start of range to query (hash value - defaults to 0x00000000)
        -e = end of range to query   (hash value - defaults to 0xFFFFFFFF)
        -c = channel name to query   (by default queries all channels in turn)

*******************************************************************************/

module src.dump;



/*******************************************************************************

    Imports

*******************************************************************************/

private import  swarm.dht.DhtClient,
                swarm.dht.DhtHash,
                swarm.dht.DhtConst;

private import swarm.dht.client.DhtNodesConfig;
private import swarm.dht.client.connection.ErrorInfo;

private import tango.io.Stdout;

private import  tango.util.Arguments;

private import  Integer = tango.text.convert.Integer;



/*******************************************************************************

    Main

*******************************************************************************/

void main ( char[][] cmdl )
{
    auto app_name = cmdl[0];

    hash_t range_min = hash_t.min;
    hash_t range_max = hash_t.max;
    char[] channel;


    // Parse command line args
    Arguments args = new Arguments(cmdl[1..$]);

    args.prefixShort = ["-"];
    args.prefixLong  = ["--"];
    
    args.define("s").parameters(0).aka("start");
    args.define("e").parameters(1).aka("end");
    args.define("c").parameters(2).aka("channel");
    
    if ( args.contains("s") && args["s"].length )
    {
        range_min = Integer.toLong(args["s"]);
    }

    if ( args.contains("e") && args["e"].length )
    {
        range_max = Integer.toLong(args["e"]);
    }

    if ( args.contains("c") && args["c"].length )
    {
        channel = args["c"];
    }

    assert(range_min <= range_max);


    // Init DHT client
    bool error;
    scope dht = new DhtClient();
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

    
    // Start query
    if ( channel.length )
    {
        dumpChannel(dht, channel, range_min, range_max);
        dht.eventLoop();
    }
    else
    {
        dht.getChannels(
            ( uint id, char[] chan)
            {
                dumpChannel(dht, chan, range_min, range_max);
            }
        ).eventLoop();
    }

    Stderr.format("Finished\n");
}



/*******************************************************************************

    Outputs dht records in the specified channel & hash range to stdout.
    
    Params:
        dht = dht client to perform query with
        channel = channel to query
        range_min = start of hash range to query
        range_max = end of hash range to query

*******************************************************************************/

void dumpChannel ( DhtClient dht, char[] channel, hash_t range_min, hash_t range_max )
{
    dht.getRange(channel, range_min, range_max,
            ( uint id, char[] key, char[] value )
            {
                if ( key.length )
                {
                    Stdout.format("{} -> {}\n", key, value);
                }
            }
        );
}

