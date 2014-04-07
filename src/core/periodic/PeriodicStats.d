/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012 2012: Initial release
                    30/05/2013: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

*******************************************************************************/

module src.core.periodic.PeriodicStats;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.core.periodic.model.IPeriodic;

private import src.core.config.StatsConfig;

private import src.mod.queue.model.IQueueNodeInfo;

private import src.mod.dht.model.IDhtNodeInfo;

private import swarm.core.node.storage.model.IStorageEngineInfo;

private import ocean.core.Array : copy, concat;

private import ocean.math.SlidingAverage;

private import ocean.text.util.DigitGrouping;

private import ocean.text.convert.Layout;

private import ocean.util.log.Stats;

private import ocean.util.log.StaticTrace;

private import tango.core.Memory;

private import swarm.core.node.model.IChannelsNodeInfo;



/*******************************************************************************

    Periodic stats class. Displays a message line to the console and writes
    lines to a stats log. Contains shared methods which both the dht and the
    queue can use.

*******************************************************************************/

public abstract class PeriodicStats : IPeriodic
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

    protected char[] records_buf, bytes_buf, memory_buf;


    /***************************************************************************

        Per-channel stats written to log file.

    ***************************************************************************/

    protected ulong[char[]] channel_stats;


    /***************************************************************************

        Titles of per-channel stats written to the log file. (These must be
        maintained separately as they are composed from the channel's name plus
        the string "_bytes" or "_records".)

    ***************************************************************************/

    protected char[][char[]] channel_bytes_title;
    protected char[][char[]] channel_records_title;


    /***************************************************************************

        Constructor.

        Params:
            stats_config = class containing configuration settings for stats
            id = identifying string of this periodic, used for logging

    ***************************************************************************/

    public this ( StatsConfig stats_config, char[] id )
    {
        this.stats_config = stats_config;

        super(console_update_time, id);

        this.records_per_sec = new SlidingAverageTime!(ulong)(5, 1_000, 1_000);

        this.log = new StatsLog(this.stats_config.file_count,
            this.stats_config.max_file_size, this.stats_config.logfile);
    }


    /***************************************************************************

        Called once every second by the base class. Updates the console display,
        and write a line to the stats log if > the specified write period (as
        specified in the construcotr) has passed.

    ***************************************************************************/

    protected void handle_ ( )
    {
        this.consoleOutput();
        this.logOutput();

        auto node_info = cast(INodeInfo)this.node;

        node_info.resetCounters();
    }


    /***************************************************************************

        Updates the console output line. Sub-classes can append additional
        information to the console output by overriding the consoleOutput_()
        method, below.

    ***************************************************************************/

    protected abstract void consoleOutput ( );


    /***************************************************************************

        Updates the counter of average records processed per second.

        Returns:
            updated average records per second

    ***************************************************************************/

    protected real recordsPerSecond ( )
    {
        auto node_info = cast(INodeInfo)this.node;

        // Update the average of records processed per second
        real rec_per_sec;

        this.records_per_sec = node_info.records_handled;

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
        auto node_info = cast(INodeInfo)this.node;

        // Update counters with bytes sent & received and records handled
        // since last call to this method
        this.log_stats.bytes_sent += node_info.bytes_sent;
        this.log_stats.bytes_received += node_info.bytes_received;
        this.log_stats.records_handled += node_info.records_handled;

        this.elapsed_since_last_log_update += this.console_update_time;

        // Output logline and reset counters when period has expired
        if ( this.elapsed_since_last_log_update >= this.log_update_time )
        {
            this.writeLogOutput();
        }
    }


    /***************************************************************************

        Write the metadata of the channel which will be logged.

    ***************************************************************************/

    protected void writeLogOutput ( )
    {
        auto node_info = cast(INodeInfo) this.node;

        this.updateChannelStats();

        this.log_stats.handling_connections = node_info.num_open_connections;

        this.log.add(this.log_stats);
        this.log.add(this.channel_stats);

        this.elapsed_since_last_log_update -= this.log_update_time;
        this.log_stats = LogStats.init;
    }


    /***************************************************************************

        Updates the channel stats associative arrays. Removes any channels from
        the stats log which no longer exist.

        Params:
            node_info = dht node info to check for channel info

    ***************************************************************************/

    protected void updateChannelStats ( )
    {
        auto node_info = cast(IChannelsNodeInfo)this.node;
        // Update existing channel stats
        foreach ( channel; node_info )
        {
            size_t bytes, records;

            //If channel id is not duplicated when added to channel_*_title and
            //later disappears from node_info(because the channel is removed),
            //channel_*_title[id] would return garbage.

            if ( !(channel.id in this.channel_bytes_title) )
            {
                this.channel_bytes_title[channel.id.dup] = channel.id ~ "_bytes";
                this.channel_stats[this.channel_bytes_title[channel.id]] = 0;
            }

            if ( !(channel.id in this.channel_records_title) )
            {
                this.channel_records_title[channel.id.dup] = channel.id ~ "_records";
                this.channel_stats[this.channel_records_title[channel.id]] = 0;
            }

            this.getChannelSize(channel, bytes, records);
            this.channel_stats[this.channel_bytes_title[channel.id]] = bytes;
            this.channel_stats[this.channel_records_title[channel.id]]= records;
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
            this.channel_stats.remove(this.channel_bytes_title[id]);
            this.channel_stats.remove(this.channel_records_title[id]);

            this.channel_bytes_title.remove(id);
            this.channel_records_title.remove(id);
        }
    }


    /***************************************************************************

        Set the out parameter bytes and records to that of the channel.

        Params:
            channel = data about a channel
            bytes   = will be set to size the channel have
            records = will be set to numer of records the channel have

    ***************************************************************************/

    protected void getChannelSize ( IStorageEngineInfo channel,
        out size_t bytes, out size_t records )
    {
        bytes   = channel.num_bytes;
        records = channel.num_records;
    }
}

