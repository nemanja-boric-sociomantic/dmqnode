/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012 2012: Initial release

    authors:        Gavin Norman

    Periodic update of stats display on the console and lines written to the
    stats log.

*******************************************************************************/

module src.core.periodic.PeriodicStats;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.core.periodic.model.IPeriodic;

private import src.core.config.StatsConfig;

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

        Stats config object, passed into constructor.

    ***************************************************************************/

    private const StatsConfig stats_config;


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

    private alias StatsLog!(LogStats) Log;
    private Log log;


    /***************************************************************************

        Console & log file update period (seconds)

    ***************************************************************************/

    private const console_update_time = 1;

    private const uint log_update_time = Log.default_period;


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

    private char[] records_buf, bytes_buf, memory_buf;


    /***************************************************************************

        Constructor.

    ***************************************************************************/

    public this ( StatsConfig stats_config )
    {
        this.stats_config = stats_config;

        super(console_update_time);

        this.records_per_sec = new SlidingAverageTime!(ulong)(5, 1_000, 1_000);

        this.log = new StatsLog!(LogStats)(this.stats_config.logfile);
    }


    /***************************************************************************

        Called once every second by the base class. Updates the console display,
        and write a line to the stats log if > the specified write period (as
        specified in the construcotr) has passed.

    ***************************************************************************/

    protected void handle_ ( )
    {
        auto node_info = cast(IDhtNodeInfo)this.dht_node;

        this.consoleOutput();
        this.logOutput();

        node_info.resetCounters();
    }


    /***************************************************************************

        Updates the console output line. Sub-classes can append additional
        information to the console output by overriding the consoleOutput_()
        method, below.

    ***************************************************************************/

    private void consoleOutput ( )
    {
        if ( this.stats_config.console_stats_enabled )
        {
            auto node_info = cast(IDhtNodeInfo)this.dht_node;

            auto rec_per_sec = this.recordsPerSecond();

            DigitGrouping.format(node_info.num_records, this.records_buf);
            BitGrouping.format(node_info.num_bytes, this.bytes_buf, "b");

            this.memory_buf.length = 0;

            version ( CDGC )
            {
                const float Mb = 1024 * 1024;
                size_t used, free;
                GC.usage(used, free);

                auto mem_allocated = cast(float)(used + free) / Mb;
                auto mem_free = cast(float)free / Mb;

                Layout!(char).print(this.memory_buf, " (Used {}Mb/Free {}Mb)",
                    mem_allocated, mem_free);
            }

            StaticTrace.format("  {} dht 0x{:X}..0x{:X}{}: {} conns, {} rec/s, {} recs ({}){}",
                node_info.storage_type, node_info.min_hash, node_info.max_hash,
                this.memory_buf, node_info.num_open_connections, rec_per_sec,
                this.records_buf, this.bytes_buf,
                this.consoleOutput_()).flush;
        }
    }

    /***************************************************************************

        Provides additional text to be displayed on the console stats line. The
        default implementation does nothing, but deriving classes can override
        in order to display additional information.

        Returns:
            text to append to console stats line

    ***************************************************************************/

    protected char[] consoleOutput_ ( )
    {
        return "";
    }


    /***************************************************************************

        Updates the counter of average records processed per second.

        Returns:
            updated average records per second

    ***************************************************************************/

    private real recordsPerSecond ( )
    {
        auto node_info = cast(IDhtNodeInfo)this.dht_node;

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
        auto node_info = cast(IDhtNodeInfo)this.dht_node;

        // Update counters with bytes sent & received and records handled
        // since last call to this method
        this.log_stats.bytes_sent += node_info.bytes_sent;
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

