/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012 2012: Initial release
                    30/05/2013: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

*******************************************************************************/

module queuenode.app.periodic.PeriodicStats;



/*******************************************************************************

    Imports

*******************************************************************************/

private import queuenode.app.periodic.model.IPeriodic;

private import queuenode.app.config.StatsConfig;

private import queuenode.storage.Ring;

private import swarm.core.node.storage.model.IStorageEngineInfo;

private import ocean.core.Array : copy, concat;

private import ocean.core.Traits: FieldName;

private import ocean.math.SlidingAverage;

private import ocean.text.util.DigitGrouping;

private import tango.text.convert.Format;

private import ocean.util.log.Stats;

private import ocean.util.log.StaticTrace;

private import tango.core.Memory;

/*******************************************************************************

    Periodic stats class. Displays a message line to the console and writes
    lines to a stats log. Contains shared methods which both the dht and the
    queue can use.

*******************************************************************************/

public class PeriodicStats : IPeriodic
{
    /***************************************************************************

        Stats config object, passed into constructor.

    ***************************************************************************/

    protected const StatsConfig stats_config;


    /***************************************************************************

        Struct containing the values to be written to the stats log file

    ***************************************************************************/

    private struct LogStats
    {
        ulong bytes_sent;
        ulong bytes_received;
        size_t handling_connections;
        ulong records_handled;
        ulong total_bytes;
        ulong total_records;
    }

    protected LogStats log_stats;


    /***************************************************************************

        Log file

    ***************************************************************************/

    protected StatsLog log;


    /***************************************************************************

        Console & log file update period (milliseconds)

    ***************************************************************************/

    private const console_update_time = 1_000;

    private const uint log_update_time = StatsLog.default_period * 1_000;


    /***************************************************************************

        Number of milliseconds elapsed since the log file was last updated

    ***************************************************************************/

    protected uint elapsed_since_last_log_update;


    /***************************************************************************

        Average records per second counter

    ***************************************************************************/

    protected SlidingAverageTime!(ulong) records_per_sec;


    /***************************************************************************

        String buffers for console output formatting.

    ***************************************************************************/

    private char[] records_buf, bytes_buf, memory_buf;

    /***************************************************************************

        Per-channel stats written to log file.

    ***************************************************************************/

    struct ChannelStats
    {
        struct Item
        {
            /*******************************************************************

                Numeric statistical value.

            *******************************************************************/

            ulong n;

            /*******************************************************************

                The title for the value to use for logging.

            *******************************************************************/

            char[] title;
        }

        /***********************************************************************

            The statistics to log.

        ***********************************************************************/

        Item  bytes, records, percent;

        /***********************************************************************

            Resets all statistical values to 0.

        ***********************************************************************/

        void reset ( )
        {
            foreach (i, ref item; this.tupleof)
            {
                item.n = 0;
            }
        }

        /***********************************************************************

            Initialises a new instance of this struct. Composes the value titles
            from the channel id and the struct field name and leaves the values
            at the default value of 0.

            Params:
                id = channel id

            Returns:
                A newly initialised instance of this struct.

        ***********************************************************************/

        static typeof(*this) opCall ( char[] id )
        {
            typeof(*this) stats;

            foreach (i, ref item; stats.tupleof)
            {
                static const suffix = "_" ~ FieldName!(i, typeof(stats));

                item.title = id ~ suffix;
            }

            return stats;
        }
    }

    /***************************************************************************

        Per-channel stats written to log file by channel name.

    ***************************************************************************/

    protected ChannelStats[char[]] channel_stats;


    /***************************************************************************

        Constructor.

        Params:
            stats_config = class containing configuration settings for stats
            epoll = epoll select dispatcher to register this periodic with (the
                registration of periodics is usually dealt with by the Periodics
                class, but an individual periodic can also reregister itself
                with epoll in the situation where an error occurs)
            id = identifying string of this periodic, used for logging

    ***************************************************************************/

    public this ( StatsConfig stats_config, EpollSelectDispatcher epoll )
    {
        super(epoll, this.console_update_time, typeof(this).stringof);

        this.stats_config = stats_config;

        this.records_per_sec = new SlidingAverageTime!(ulong)(5, 1_000, 1_000);

        this.log = new StatsLog(this.stats_config.file_count,
            this.stats_config.max_file_size, this.stats_config.logfile);
    }


    /***************************************************************************

        Called once every second by the base class. Updates the console display,
        and write a line to the stats log if > the specified write period (as
        specified in the construcotr) has passed.

    ***************************************************************************/

    override protected void run ( )
    {
        this.updateChannelStats();

        if (this.stats_config.console_stats_enabled)
        {
            this.consoleOutput();
        }

        this.logOutput();

        this.node_info.resetCounters();

        foreach (ref stats; this.channel_stats)
        {
            stats.reset();
        }
    }

    /***************************************************************************

        Updates the console output line.

    ***************************************************************************/

    protected void consoleOutput ( )
    {
        this.updateChannelStats();

        if ( this.stats_config.console_stats_enabled )
        {
            DigitGrouping.format(this.node_info.num_records, this.records_buf);
            BitGrouping.format(this.node_info.num_bytes, this.bytes_buf, "b");

            this.memory_buf.length = 0;

            const float Mb = 1024 * 1024;
            size_t used, free;
            GC.usage(used, free);

            if ( used + free > 0 )
            {
                auto mem_allocated = cast(float)(used + free) / Mb;
                auto mem_free = cast(float)free / Mb;

                Format.format(this.memory_buf, " (Used {}Mb/Free {}Mb)",
                    mem_allocated, mem_free);
            }
            else
            {
                Format.format(this.memory_buf, " (mem usage n/a)");
            }

        }

        StaticTrace.format("  {} queue {}: {} conns, {} rec/s, {} recs ({})",
            this.node_info.storage_type, this.memory_buf,
            this.node_info.num_open_connections,
            this.recordsPerSecond(), this.records_buf, this.bytes_buf).flush;
    }

    /***************************************************************************

        Updates the counter of average records processed per second.

        Returns:
            updated average records per second

    ***************************************************************************/

    private real recordsPerSecond ( )
    {
        // Update the average of records processed per second
        real rec_per_sec;

        this.records_per_sec = this.node_info.records_handled;

        for ( int i; i < this.console_update_time / 1_000; i++ )
        {
            rec_per_sec = this.records_per_sec.push;
        }

        return rec_per_sec;
    }


    /***************************************************************************

        Gathers stats to write to the log file, and writes a line to the stats
        log is the output period (passed to the constructor) has expired.

    ***************************************************************************/

    protected void logOutput ( )
    {
        // Update counters with bytes sent & received and records handled
        // since last call to this method
        this.log_stats.bytes_sent += this.node_info.bytes_sent;
        this.log_stats.bytes_received += this.node_info.bytes_received;
        this.log_stats.records_handled += this.node_info.records_handled;

        this.elapsed_since_last_log_update += this.console_update_time;

        // Output logline and reset counters when period has expired
        if ( this.elapsed_since_last_log_update >= this.log_update_time )
        {
            this.log_stats.total_bytes = this.node_info.num_bytes;
            this.log_stats.total_records = this.node_info.num_records;
            this.log_stats.handling_connections = this.node_info.num_open_connections;

            this.log.add(this.log_stats);

            foreach (stats; this.channel_stats)
            {
                foreach (item; stats.tupleof)
                {
                    this.log.add(item.title, item.n);
                }
            }

            this.log.flush();

            this.elapsed_since_last_log_update -= this.log_update_time;
            this.log_stats = LogStats.init;
        }
    }


    /***************************************************************************

        Updates the channel stats associative arrays. Removes any channels from
        the stats log which no longer exist.

    ***************************************************************************/

    private void updateChannelStats ( )
    {
        // Update existing channel stats
        foreach ( channel_info; this.node_info )
        {
            auto channel = cast(RingNode.Ring)channel_info;
            assert(channel, "channel class " ~ channel.classinfo.name ~ " is not " ~ RingNode.Ring.stringof);
            //If channel id is not duplicated when added to channel_*_title and
            //later disappears from node_info(because the channel is removed),
            //channel_*_title[id] would return garbage.

            if (ChannelStats* stats = channel.id in this.channel_stats)
            {
                if (stats.bytes.n < channel.num_bytes)
                {
                    stats.bytes.n = channel.num_bytes;
                }

                if (stats.records.n < channel.num_records)
                {
                    stats.records.n = channel.num_records;
                }

                // If a channel has a zero capacity, don't divide by it but
                // report 100% usage. This is currently not allowed as a
                // configuration value but making it robust doesn't hurt.
                if (ulong capacity_bytes = channel.capacity_bytes)
                {
                    stats.percent.n = cast(ulong)(stats.bytes.n * 100.f / capacity_bytes);
                }
                else
                {
                    stats.percent.n = 100;
                }
            }
            else
            {
                this.channel_stats[channel.id] = ChannelStats(channel.id);
            }
        }

        // Check for dead channels in stats. One shouldn't remove elements while
        // iterating over an associative array so we have to make a list of
        // removed channels first, then iterate over that list and remove the
        // channels from this.channel_stats.
        char[][] to_remove; // Note: this will allocate in the case where a
            // channel has been removed. This is very rare.
        foreach ( id, stats; this.channel_stats )
        {
            if ( !(id in node_info) )
            {
                to_remove ~= id;
            }
        }

        // Remove dead channels from stats
        foreach ( id; to_remove )
        {
            this.channel_stats.remove(id);
        }
    }
}

