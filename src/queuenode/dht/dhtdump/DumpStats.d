/*******************************************************************************

    Copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

    Stats aggregator and stats.log writer for dump cycle.

*******************************************************************************/

module queuenode.dht.dhtdump.DumpStats;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.core.Array : concat;

private import ocean.util.log.Stats;

private import ocean.io.select.EpollSelectDispatcher;



public class DumpStats
{
    /***************************************************************************

        Alias for the periodic stats logger config class.

    ***************************************************************************/

    public alias IPeriodicStatsLog.Config Config;


    /***************************************************************************

        Stats logging class, registered with epoll in super class' ctor. Writes
        gathered stats to a log file periodically.

    ***************************************************************************/

    private class StatsLog : IPeriodicStatsLog
    {
        /***********************************************************************

            Buffer used for string formatting in addStats().

        ***********************************************************************/

        private char[] suffix_buffer;


        /***********************************************************************

            Constructor. Registers an update timer with epoll.

            Params:
                config = periodic stats config parameters
                epoll = epoll instance to register update timer with

        ***********************************************************************/

        public this ( Config config, EpollSelectDispatcher epoll )
        {
            super(epoll, config);
        }


        /***********************************************************************

            Called periodically by the super class. Adds the aggregated stats to
            the stats log and resets the counters.

        ***********************************************************************/

        protected override void addStats ( )
        {
            this.stats_log.add(this.outer.current_stats);
            foreach ( channel, stats; this.outer.channel_stats )
            {
                this.suffix_buffer.concat("_", channel);
                this.stats_log.addSuffix(stats, this.suffix_buffer);
            }

            this.outer.current_stats = this.outer.current_stats.init;
        }
    }


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

        Constructor. Registers an update timer with epoll which writes the stats
        to the log periodically.

        Params:
            config = periodic stats config parameters
            epoll = epoll instance to register update timer with

    ***************************************************************************/

    public this ( Config config, EpollSelectDispatcher epoll )
    {
        new StatsLog(config, epoll);
    }


    /***************************************************************************

        Constructor. Does not register an update timer with epoll.

        Params:
            config = periodic stats config parameters
            epoll = epoll instance to register update timer with

    ***************************************************************************/

    public this ( )
    {
    }


    /***************************************************************************

        Should be called when a record has been dumped. Updates the stats
        counters with the amount of data written to disk for this record.

        Params:
            key = key of record dumped
            value = value of record dumped

    ***************************************************************************/

    public void dumpedRecord ( char[] key, char[] value )
    {
        this.current_stats.records_written++;
        // bytes of key, value, and length specifiers of each
        this.current_stats.bytes_written += key.length + value.length
            + (size_t.sizeof * 2);
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

        Returns:
            the total number of bytes written to all channels during the last
            cycle

    ***************************************************************************/

    public ulong total_bytes ( )
    {
        ulong sum;
        foreach ( channel; this.channel_stats )
        {
            sum += channel.bytes_written;
        }
        return sum;
    }


    /***************************************************************************

        Returns:
            the total number of records written to all channels during the last
            cycle

    ***************************************************************************/

    public ulong total_records ( )
    {
        ulong sum;
        foreach ( channel; this.channel_stats )
        {
            sum += channel.records_written;
        }
        return sum;
    }
}

