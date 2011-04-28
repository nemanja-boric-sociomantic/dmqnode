module src.mod.ranges.DhtRanges;



private import src.mod.model.DhtTool;

private import ocean.core.Array;

private import ocean.core.ArrayMap;

private import ocean.util.log.PeriodicTrace;

private import ocean.text.Arguments;

private import swarm.dht.DhtClient;

private import tango.core.Array;

private import Integer = tango.text.convert.Integer;

private import tango.io.Stdout;



class DhtRanges : DhtTool
{
    /***************************************************************************
    
        Singleton parseArgs() and run() methods.
    
    ***************************************************************************/
    
    mixin SingletonMethods;


    protected void process_ ( DhtClient dht )
    {
        if ( this.all_channels )
        {
            this.processAllChannels(dht);
        }
        else
        {
            this.processChannel(dht, this.channel);
        }
    }

    override protected void addArgs_ ( Arguments args )
    {
        args("source").params(1).required().aliased('S').help("path of dhtnodes.xml file defining nodes to dump");
        args("channel").conflicts("all_channels").params(1).aliased('c').help("channel name to query");
        args("all_channels").conflicts("channel").aliased('A').help("query all channels");
    }

    override protected bool validArgs ( Arguments args )
    {
        if ( !args.exists("source") )
        {
            Stderr.formatln("No xml source file specified (use -S)");
            return false;
        }
        
        bool all_channels = args.exists("all_channels");
        bool one_channel = args.exists("channel");
    
        if ( !(all_channels || one_channel) )
        {
            Stderr.formatln("Please specify exactly one of the following options: single channel (-c) or all channels (-A)");
            return false;
        }
    
        return true;
    }

    private bool all_channels;
    private char[] channel;

    override protected void readArgs ( Arguments args )
    {
        super.dht_nodes_config = args.getString("source");

        if ( args.exists("all_channels") )
        {
            this.all_channels = true;
            this.channel.length = 0;
        }
        else if ( args.exists("channel") )
        {
            this.all_channels = false;
            this.channel = args.getString("channel");
        }
    }

    private ArrayMap!(uint, hash_t) records;

    
    public this ( )
    {
        this.records = new ArrayMap!(uint, hash_t);
    }

    private void processChannel ( DhtClient dht, char[] channel )
    {
        ulong record_count;
        this.records.clear();

        dht.getAllKeys(channel,
                ( DhtClient.RequestContext context, char[] key )
                {
                    if ( key.length )
                    {
                        auto hash = Integer.toInt(key, 16);
                        if ( hash in this.records )
                        {
                            auto current = this.records[hash];
                            this.records[hash] = current + 1;
                        }
                        else
                        {
                            this.records[hash] = 1;
                        }
                        StaticPeriodicTrace.format("{}: {}", channel, ++record_count);
                    }
                }
            ).eventLoop;

        // Sorted list of keys
        hash_t[] keys;
        foreach ( hash, count; this.records )
        {
            keys ~= hash;
        }
        keys.sort;

        if ( keys.length > 1 )
        {
            Stdout.formatln("Channel '{}' has {} records and {} keys (avg. of {} records per key)",
                    channel, record_count, keys.length, record_count / keys.length);
            Stdout.formatln("   First key = 0x{:x8}, Last key = 0x{:x8}", keys.length, keys[0], keys[$-1]);
    
            // Display clusters
            const cluster_gap = 1024;
            hash_t cluster_start = keys[0];
            hash_t cluster_end = cluster_start;
    
            foreach ( hash; keys[1..$] )
            {
                if ( hash > cluster_end + cluster_gap )
                {
                    Stdout.formatln("Cluster 0x{:x8} .. 0x{:x8} (range length = {})", cluster_start, cluster_end, cluster_end - cluster_start);
                    cluster_start = hash;
                }
    
                cluster_end = hash;
            }
            Stdout.formatln("Cluster 0x{:x8} .. 0x{:x8} (range length = {})", cluster_start, cluster_end, cluster_end - cluster_start);
    
            // Visual display
    /*        const num_chunks = 64 * 64;
            const chunk_size = hash_t.max / num_chunks;
            ulong[num_chunks] chunk_count;
            foreach ( hash, count; this.records )
            {
                auto chunk = hash / chunk_size;
                chunk_count[chunk] += count;
            }
    
            uint row_count;
            foreach ( count; chunk_count )
            {
                if ( count > 1000 )
                {
                    Stdout.format("O");
                }
                else if ( count > 100 )
                {
                    Stdout.format("o");
                }
                else
                {
                    Stdout.format(".");
                }
    
                if ( ++row_count >= 64 )
                {
                    Stdout.formatln("");
                    row_count = 0;
                }
            }*/
        }
        else
        {
            Stdout.formatln("Channel '{}' has 0 records", channel);
        }
    }


    private void processAllChannels ( DhtClient dht )
    {
        char[][] channels;
        dht.getChannels(
                ( DhtClient.RequestContext context, char[] channel )
                {
                    if ( channel.length && !channels.contains(channel) )
                    {
                        channels.appendCopy(channel);
                    }
                }
            ).eventLoop();

        foreach ( channel; channels )
        {
            this.processChannel(dht, channel);
        }
    }
}

