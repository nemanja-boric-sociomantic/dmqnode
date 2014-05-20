/*******************************************************************************

    Copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

    Stats aggregator and stats.log writer for dump cycle.

*******************************************************************************/

module swarmnodes.dht.memory.dhtdump.DumpStats;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.core.Array : concat;

private import ocean.util.log.Stats;

private import ocean.io.select.EpollSelectDispatcher;



public class DumpStats : IPeriodicStatsLog
{
    /***************************************************************************

        Struct wrapping the set of stats to be recorded about a dump cycle.

    ***************************************************************************/

    private struct IOStats
    {
        ulong records_written;
        ulong bytes_written;
    }


    /***************************************************************************

        Total data written since the last log update. Cleared after updating.

    ***************************************************************************/

    private IOStats current_stats;


    /***************************************************************************

        Data written per channel. Only updated *after* a channel has been
        completely dumped. Never cleared.

    ***************************************************************************/

    private IOStats[char[]] channel_stats;


    /***************************************************************************

        Buffer used for string formatting in addStats().

    ***************************************************************************/

    private char[] suffix_buffer;


    /***************************************************************************

        Constructor. Registers an update timer with epoll.

        Params:
            config = periodic stats config parameters
            epoll = epoll instance to register update timer with

    ***************************************************************************/

    public this ( Config config, EpollSelectDispatcher epoll )
    {
        super(epoll, config);
    }


    /***************************************************************************

        Should be called when a record has been dumped. Updates the stats
        counters.

        Params:
            key = key of record dumped
            value = value of record dumped

    ***************************************************************************/

    public void dumpedRecord ( char[] key, char[] value )
    {
        this.current_stats.records_written++;
        this.current_stats.bytes_written += key.length + value.length;
    }


    /***************************************************************************

        Should be called when a channel has been dumped. Updates the stats
        counters.

        Params:
            channel = name of channel which was dumped
            records = total number of records in channel
            bytes = total number of bytes in channel

    ***************************************************************************/

    public void dumpedChannel ( char[] channel, ulong records, ulong bytes )
    {
        if ( !(channel in this.channel_stats) )
        {
            this.channel_stats[channel] = IOStats();
        }

        this.channel_stats[channel].records_written = records;
        this.channel_stats[channel].bytes_written = bytes;
    }


    /***************************************************************************

        Called periodically by the super class. Adds the aggregated stats to the
        stats log.

    ***************************************************************************/

    protected override void addStats ( )
    {
        this.stats_log.add(this.current_stats);
        foreach ( channel, stats; this.channel_stats )
        {
            this.suffix_buffer.concat("_", channel);
            this.stats_log.addSuffix(stats, this.suffix_buffer);
        }

        this.current_stats = this.current_stats.init;
    }
}

