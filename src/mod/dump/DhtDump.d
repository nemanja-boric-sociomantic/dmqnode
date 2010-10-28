/*******************************************************************************

    DHT node dump
    
    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        October 2010: Initial release
    
    authors:        Gavin Norman
    
    Reads records from one or more dht nodes and outputs them to stdout.
    
    Command line parameters:
        -h = display help
        -S = dhtnodes.xml source file
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

private import ocean.core.Array;

debug private import tango.util.log.Trace;



/*******************************************************************************

    Dht dump struct - initialises and executes a dht dump from command line
    arguments.

*******************************************************************************/

struct DhtDump
{
static:

    /***************************************************************************

        Internal count of records & bytes received
    
    ***************************************************************************/

    private ulong records, bytes;

    
    /***************************************************************************

        Runs the dht node dump
        
        Params:
            args = command line arguments
            
        Throws:
            asserts that the node(s) support(s) getRange, if the arguments
            indicate that a getRange request is needed

    ***************************************************************************/

    public bool run ( Arguments args )
    {
        auto xml = args.getString("source");
        auto range_min = args.getInt!(hash_t)("start");
        auto range_max = args.getInt!(hash_t)("end");
        auto channel = args.getString("channel");
        auto all_channels = args.getBool("all_channels");
        auto count_records = args.getBool("count");

        scope dht = initDhtClient(xml);

        if ( all_channels )
        {
            dumpAllChannels(dht, range_min, range_max, count_records);
        }
        else
        {
            dumpChannel(dht, channel, range_min, range_max, count_records);
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

    private DhtClient initDhtClient ( char[] xml )
    {
        bool error;
        auto dht = new DhtClient();
        
        dht.error_callback(
            ( ErrorInfo e )
            {
                Stderr.format("DHT client error: {}\n", e.message);
                error = true;
            }
        );
        
        DhtNodesConfig.addNodesToClient(dht, xml);
        dht.nodeHandshake();
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
            count_records = whether to display a count of the records without
                dumping the record contents
    
    ***************************************************************************/

    private void dumpAllChannels ( DhtClient dht, hash_t range_min, hash_t range_max, bool count_records )
    {
        records = 0;
        bytes = 0;
        
        char[][] channels;
        dht.getChannels(
                ( uint id, char[] channel )
                {
                    if ( channel.length )
                    {
                        channels.appendCopy(channel);
                    }
                }
            ).eventLoop();

        foreach ( channel; channels )
        {
            dumpChannel(dht, channel, range_min, range_max, count_records);
        }

        if ( count_records )
        {
            Stdout.format("Total of all channels = {} records ({} bytes) in the specified range\n", records, bytes);
        }
    }


    /***************************************************************************

        Outputs dht records in the specified channel & hash range to stdout.
        
        Params:
            dht = dht client to perform query with
            channel = channel to query
            range_min = start of hash range to query
            range_max = end of hash range to query
            count_records = whether to display a count of the records without
                dumping the record contents

    ***************************************************************************/
    
    private void displayChannelSize ( DhtClient dht, char[] channel  )
    {
        dht.getChannelSize(channel, (hash_t id, char[] address, ushort port, char[] channel, 
                                     ulong records, ulong bytes)
                          {
                              Stdout.formatln("{}:{} {} - {} records, {} bytes", address, port, channel, records, bytes);
                          }).eventLoop();
        Stdout.flush();
    }
    
    
    /***************************************************************************

        Outputs dht records in the specified hash range in the specified
        channel to stdout.
        
        Params:
            dht = dht client to perform query with
            channel = name of channel to dump
            range_min = start of hash range to query
            range_max = end of hash range to query
            count_records = whether to display a count of the records without
                dumping the record contents
    
    ***************************************************************************/

    private void dumpChannel ( DhtClient dht, char[] channel, hash_t range_min, hash_t range_max, bool count_records )
    {
        ulong channel_records, channel_bytes;

        displayChannelSize(dht, channel);
        
        void receiveRecord ( uint id, char[] key, char[] value )
        {
            if ( key.length )
            {
                channel_records++;
                channel_bytes += value.length;
                if ( !count_records )
                {
                    Stdout.format("{}: {} -> {}\n", channel, key, value);
                }
                else if (!(channel_records % 10_000))
                {
                    Stdout.format("\b\b\b\b\b\b\b\b\b\b{,10}", channel_records).flush();
                }
            }
        }

        if ( dht.commandSupported(DhtConst.Command.GetRange) )
        {
            dht.getRange(channel, range_min, range_max, &receiveRecord).eventLoop();
        }
        else
        {
            dht.getAll(channel,
                    ( uint id, char[] hash_str, char[] value )
                    {
                        if ( hash_str.length )
                        {
                            auto hash = DhtHash.straightToHash(hash_str);
                            if ( hash >= range_min && hash <= range_max )
                            {
                                receiveRecord(id, hash_str, value);
                            }
                        }
                    }).eventLoop();
        }

        records += channel_records;
        bytes += channel_bytes;
        if ( count_records )
        {
            Stdout.format("Channel {} contains {} records ({} bytes) in the specified range\n\n", channel, channel_records, channel_bytes);
        }
    }
}

