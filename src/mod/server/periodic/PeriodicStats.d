/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012 2012: Initial release

    authors:        Gavin Norman

    Periodic update of stats display on the console and lines written to the
    stats log.

*******************************************************************************/

module src.mod.server.periodic.PeriodicStats;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.server.periodic.model.IPeriodic;

private import src.mod.server.config.MainConfig;

private import ocean.math.SlidingAverage;

private import ocean.text.util.DigitGrouping;

private import ocean.text.convert.Layout;

private import ocean.util.log.Stats;

private import ocean.util.log.StaticTrace;

private import tango.core.Memory;



/*******************************************************************************

    Periodic stats class. Displays a message line to the console and writes
    lines to a stats log.

*******************************************************************************/

public class PeriodicStats : IPeriodic
{
    /***************************************************************************

        Struct containing the values to be written to the stats log file

    ***************************************************************************/

    private struct DhtStats
    {
        ulong bytes_sent;
        float Kb_sent_per_sec;
        ulong bytes_received;
        float Kb_received_per_sec;
        size_t handling_connections;
        real records_per_sec;
    }


    /***************************************************************************

        Log file

    ***************************************************************************/

    private StatsLog!(DhtStats) log;


    /***************************************************************************

        Console & log file update period (seconds)

    ***************************************************************************/

    private const console_update_time = 1;

    private uint log_update_time;


    /***************************************************************************
    
        Number of seconds elapsed since the log file was last updated

    ***************************************************************************/

    private uint elapsed_since_last_log_update;


    /***************************************************************************

        Count of bytes sent & received since the log file was last updated

    ***************************************************************************/

    private ulong total_sent;

    private ulong total_received;


    /***************************************************************************

        Average records per second counter

    ***************************************************************************/

    private SlidingAverageTime!(ulong) records_per_sec;


    /***************************************************************************

        String buffers for console output formatting.

    ***************************************************************************/

    private char[] records_buf, bytes_buf;


    /***************************************************************************

        Channel sizes string buffer for console output.

    ***************************************************************************/

    private char[] channel_sizes_buf;


    /***************************************************************************

        Associative array of channel sizes (in terms of % full) indexed by name,
        for stats log output.

    ***************************************************************************/

    private float[char[]] channel_sizes;


    /***************************************************************************

        Constructor.

        Params:
            log_update_time = seconds between updates of the stats log (the
                console output is udpated every second)

    ***************************************************************************/

    public this ( uint log_update_time )
    {
        super(this.console_update_time);

        this.records_per_sec = new SlidingAverageTime!(ulong)(5, 1_000, 1_000);

        this.log_update_time = log_update_time;

        if ( MainConfig.log.stats_log_enabled )
        {
            this.log = new StatsLog!(DhtStats);
        }
    }


    /***************************************************************************

        Called once every second by the base class. Updates the console display,
        and write a line to the stats log if > the specified write period (as
        specified in the construcotr) has passed.

    ***************************************************************************/

    protected void handle_ ( )
    {
        auto node_info = cast(IQueueNodeInfo)this.node;

        auto rec_per_sec = this.recordsPerSecond();

        this.consoleOutput(rec_per_sec);
        this.logOutput(rec_per_sec);

        node_info.resetCounters();
    }


    /***************************************************************************

        Updates the counter of average records processed per second.

        Returns:
            updated average records per second

    ***************************************************************************/

    private real recordsPerSecond ( )
    {
        auto node_info = cast(IQueueNodeInfo)this.node;

        // Update the average of records processed per second
        real rec_per_sec;

        this.records_per_sec = node_info.records_handled;

        for ( int i; i < this.console_update_time; i++ )
        {
            rec_per_sec = this.records_per_sec.push;
        }

        return rec_per_sec;
    }


    /***************************************************************************

        Updates the console output line.

        Params:
            rec_per_sec = average records per second

    ***************************************************************************/

    private void consoleOutput ( real rec_per_sec )
    {
        if ( MainConfig.log.console_stats_enabled )
        {
            auto node_info = cast(IQueueNodeInfo)this.node;

            auto channels_string = this.channelSizesString(node_info);

            DigitGrouping.format(node_info.num_records, this.records_buf);
            BitGrouping.format(node_info.num_bytes, this.bytes_buf, "b");

            version ( CDGC )
            {
                const float Mb = 1024 * 1024;
                size_t used, free;
                GC.usage(used, free);

                auto mem_allocated = cast(float)(used + free) / Mb;
                auto mem_free = cast(float)free / Mb;

                StaticTrace.format("  {} queue (Used {}Mb/Free {}Mb): handling {} connections, {} records/s, {} records ({}){}",
                        node_info.storage_type, mem_allocated, mem_free,
                        node_info.num_open_connections, rec_per_sec,
                        this.records_buf, this.bytes_buf, channels_string).flush;
            }
            else
            {
                StaticTrace.format("  {} queue: handling {} connections, {} records/s, {} records ({}){}",
                        node_info.storage_type,
                        node_info.num_open_connections, rec_per_sec,
                        this.records_buf, this.bytes_buf, channels_string).flush;
            }
        }
    }


    /***************************************************************************

        Writes a line to the stats log is the output period (passed to the
        constructor) has expired.

        Params:
            rec_per_sec = average records per second

    ***************************************************************************/

    private void logOutput ( real rec_per_sec )
    {
        if ( MainConfig.log.stats_log_enabled )
        {
            auto node_info = cast(IQueueNodeInfo)this.node;

            // Bytes sent & received since last call
            auto received = node_info.bytes_received;
            auto sent = node_info.bytes_sent;

            // Update counts
            this.total_sent += sent;
            this.total_received += received;

            this.elapsed_since_last_log_update += this.console_update_time;

            // Output logline when period has expired
            if ( this.elapsed_since_last_log_update >= this.log_update_time )
            {
                DhtStats stats;
                stats.bytes_sent = sent;
                stats.Kb_sent_per_sec =
                    cast(float)(this.total_sent / 1024)
                    / cast(float)this.elapsed_since_last_log_update;

                stats.bytes_received = received;
                stats.Kb_received_per_sec =
                    cast(float)(this.total_received / 1024)
                    / cast(float)this.elapsed_since_last_log_update;

                stats.handling_connections = node_info.num_open_connections;
                stats.records_per_sec = rec_per_sec;

                this.updateChannelSizes(node_info);
                this.log.writeExtra(stats, this.channel_sizes);

                this.elapsed_since_last_log_update -= this.elapsed_since_last_log_update;
                this.total_sent = 0;
                this.total_received = 0;
            }
        }
    }


    /***************************************************************************

        Formats the current size of each channel (in terms of % full) into the
        channel_sizes_buf member. Used by the console output.

        Params:
            node_info = node information interface

    ***************************************************************************/

    private char[] channelSizesString ( IQueueNodeInfo node_info )
    {
        this.channel_sizes_buf.length = 0;

        foreach ( channel_info; node_info )
        {
            auto percent = (cast(float)channel_info.num_bytes /
                            cast(float)node_info.channelSizeLimit) * 100.0;
            Layout!(char).print(this.channel_sizes_buf, ", {}: {}%",
                channel_info.id, percent);
        }

        return this.channel_sizes_buf;
    }


    /***************************************************************************

        Updates the channel_sizes associative array with the current size of
        each channel (in terms of % full). Used by the stats log output.

        Params:
            node_info = node information interface

    ***************************************************************************/

    private void updateChannelSizes ( IQueueNodeInfo node_info )
    {
        foreach ( channel_info; node_info )
        {
            auto percent = (cast(float)channel_info.num_bytes /
                            cast(float)node_info.channelSizeLimit) * 100.0;
            this.channel_sizes[channel_info.id] = percent;
        }
    }
}

