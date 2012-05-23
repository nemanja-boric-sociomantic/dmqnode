/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012 2012: Initial release

    authors:        Gavin Norman

    Periodic update of stats display on the console and lines written to the
    stats log.

*******************************************************************************/

module src.mod.node.periodic.PeriodicStats;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.node.periodic.model.IPeriodic;

private import src.mod.node.config.MainConfig;

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

    private struct LogStats
    {
        ulong bytes_sent;
        ulong bytes_received;
        size_t handling_connections;
        ulong records_handled;
    }

    private LogStats log_stats;


    /***************************************************************************

        Log file

    ***************************************************************************/

    private StatsLog!(LogStats) log;


    /***************************************************************************

        Console & log file update period (seconds)

    ***************************************************************************/

    private const console_update_time = 1;

    private const uint log_update_time;


    /***************************************************************************
    
        Number of seconds elapsed since the log file was last updated

    ***************************************************************************/

    private uint elapsed_since_last_log_update;


    /***************************************************************************

        Average records per second counter

    ***************************************************************************/

    private SlidingAverageTime!(ulong) records_per_sec;


    /***************************************************************************

        String buffers for console output formatting.

    ***************************************************************************/

    private char[] records_buf, bytes_buf;


    /***************************************************************************

        Constructor.

        Params:
            log_update_time = seconds between updates of the stats log (the
                console output is udpated every second)

    ***************************************************************************/

    public this ( uint log_update_time )
    {
        super(console_update_time);

        this.records_per_sec = new SlidingAverageTime!(ulong)(5, 1_000, 1_000);

        this.log_update_time = log_update_time;

        if ( MainConfig.log.stats_log_enabled )
        {
            this.log = new StatsLog!(LogStats)(MainConfig.log.stats);
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

        this.consoleOutput();
        this.logOutput();

        node_info.resetCounters();
    }


    /***************************************************************************

        Updates the console output line.

    ***************************************************************************/

    private void consoleOutput ( )
    {
        if ( MainConfig.log.console_stats_enabled )
        {
            auto node_info = cast(IQueueNodeInfo)this.node;

            auto rec_per_sec = this.recordsPerSecond();

            DigitGrouping.format(node_info.num_records, this.records_buf);
            BitGrouping.format(node_info.num_bytes, this.bytes_buf, "b");

            version ( CDGC )
            {
                const float Mb = 1024 * 1024;
                size_t used, free;
                GC.usage(used, free);

                auto mem_allocated = cast(float)(used + free) / Mb;
                auto mem_free = cast(float)free / Mb;

                StaticTrace.format("  {} queue (Used {}Mb/Free {}Mb): handling {} connections, {} records/s, {} records ({})",
                        node_info.storage_type, mem_allocated, mem_free,
                        node_info.num_open_connections, rec_per_sec,
                        this.records_buf, this.bytes_buf).flush;
            }
            else
            {
                StaticTrace.format("  {} queue: handling {} connections, {} records/s, {} records ({})",
                        node_info.storage_type,
                        node_info.num_open_connections, rec_per_sec,
                        this.records_buf, this.bytes_buf).flush;
            }
        }
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

        Gethers stats to write to the log file, and writes a line to the stats
        log is the output period (passed to the constructor) has expired.

    ***************************************************************************/

    private void logOutput ( )
    {
        if ( MainConfig.log.stats_log_enabled )
        {
            auto node_info = cast(IQueueNodeInfo)this.node;

            // Update counters with bytes sent & received and records handled
            // since last call to this method
            this.log_stats.bytes_received += node_info.bytes_sent;
            this.log_stats.bytes_received += node_info.bytes_received;
            this.log_stats.records_handled += node_info.records_handled;

            this.elapsed_since_last_log_update += this.console_update_time;

            // Output logline and reset counters when period has expired
            if ( this.elapsed_since_last_log_update >= this.log_update_time )
            {
                this.log_stats.handling_connections = node_info.num_open_connections;

                this.log.write(this.log_stats);

                this.elapsed_since_last_log_update -= this.elapsed_since_last_log_update;
                this.log_stats = LogStats.init;
            }
        }
    }
}

