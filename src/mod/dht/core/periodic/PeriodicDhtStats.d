/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012 2012: Initial release
                    30/05/2013: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

    Periodic update of stats display on the console and lines written to the
    stats log.

*******************************************************************************/

module src.mod.dht.core.periodic.PeriodicDhtStats;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.core.periodic.PeriodicStats;

private import src.core.config.StatsConfig;

private import src.mod.dht.model.IDhtNodeInfo;

private import swarm.core.node.storage.model.IStorageEngineInfo;

private import ocean.core.Array : copy, concat;

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

public class PeriodicDhtStats : PeriodicStats
{
    protected alias .IDhtNodeInfo IDhtNodeInfo;


    public this ( StatsConfig stats_config )
    {
        super(stats_config, typeof(this).stringof);
    }


    /***************************************************************************

        Updates the console output line. Sub-classes can append additional
        information to the console output by overriding the consoleOutput_()
        method, below.

    ***************************************************************************/

    protected override void consoleOutput ( )
    {
        if ( this.stats_config.console_stats_enabled )
        {
            auto node_info = cast(IDhtNodeInfo)this.node;

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

            StaticTrace.format("  {} dht 0x{:X16}..0x{:X16}{}: {} conns, {} rec/s, {} recs ({}){}",
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

        Gathers stats to write to the log file, and writes a line to the stats
        log is the output period (passed to the constructor) has expired.

    ***************************************************************************/

    protected override void logOutput ( )
    {
        auto node_info = cast(IDhtNodeInfo)this.node;

        // Update counters with bytes sent & received and records handled
        // since last call to this method
        this.log_stats.bytes_sent += node_info.bytes_sent;
        this.log_stats.bytes_received += node_info.bytes_received;
        this.log_stats.records_handled += node_info.records_handled;

        this.elapsed_since_last_log_update += this.console_update_time;

        // Output logline and reset counters when period has expired
        if ( this.elapsed_since_last_log_update >= this.log_update_time )
        {
            this.updateChannelStats();

            this.log_stats.total_bytes = node_info.num_bytes;
            this.log_stats.total_records = node_info.num_records;
            this.log_stats.handling_connections = node_info.num_open_connections;

            this.log.add(this.log_stats);
            this.log.add(this.channel_stats);

            this.elapsed_since_last_log_update -= this.log_update_time;
            this.log_stats = LogStats.init;
        }
    }


    /***************************************************************************

        Updates the channel stats associative arrays. Removes any channels from
        the stats log which no longer exist in the dht.

        Params:
            node_info = dht node info to check for channel info

    ***************************************************************************/

    protected override void updateChannelStats ( )
    {
        auto node_info = cast(IDhtNodeInfo)this.node;
        // Update existing channel stats
        foreach ( channel; node_info )
        {
            if ( !(channel.id in this.channel_bytes_title) )
            {
                this.channel_bytes_title[channel.id] = channel.id ~ "_bytes";
            }
            this.channel_stats[this.channel_bytes_title[channel.id]]
                = channel.num_bytes;

            if ( !(channel.id in this.channel_records_title) )
            {
                this.channel_records_title[channel.id] = channel.id ~ "_records";
            }
            this.channel_stats[this.channel_records_title[channel.id]]
                = channel.num_records;
        }

        // Check for dead channels in stats
        char[][] to_remove; // Note: this will allocate in the case where a
            // channel has been removed. This is very rare.
        foreach ( id, title; this.channel_bytes_title )
        {
            // Sanity check that the two lists of titles are the same
            assert(id in this.channel_records_title, "Records/bytes title mismatch");

            if ( !(id in node_info) )
            {
                to_remove ~= id;
            }
        }

        // Remove dead channels from stats
        foreach ( id; to_remove )
        {
            this.channel_bytes_title.remove(id);
            this.channel_records_title.remove(id);

            this.records_buf.concat(id, "_bytes");
            this.channel_stats.remove(this.records_buf);

            this.records_buf.concat(id, "_records");
            this.channel_stats.remove(this.records_buf);
        }
    }
}

