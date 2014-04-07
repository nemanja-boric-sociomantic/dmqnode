/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012: Initial release
                    30/05/2013: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

    Periodic update of stats display on the console and lines written to the
    stats log.

*******************************************************************************/

module src.mod.queue.periodic.PeriodicQueueStats;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.core.periodic.PeriodicStats;

private import src.core.config.StatsConfig;

private import src.mod.queue.model.IQueueNodeInfo;

private import swarm.core.node.storage.model.IStorageEngineInfo;

private import ocean.core.Array : copy, concat;

private import ocean.math.SlidingAverage;

private import ocean.text.util.DigitGrouping;

private import ocean.text.convert.Layout;

private import ocean.util.log.Stats;

private import ocean.util.log.StaticTrace;

private import tango.core.Memory;

private import tango.util.MinMax;


/*******************************************************************************

    Periodic stats class. Displays a message line to the console and writes
    lines to a stats log.

*******************************************************************************/

public class PeriodicQueueStats : PeriodicStats
{
    /***************************************************************************

        Constructor.

        Params:
            stats_config = class containing configuration settings for stats

    ***************************************************************************/

    public this ( StatsConfig stats_config )
    {
        super(stats_config, typeof(this).stringof);
    }


    /***************************************************************************

        Updates the console output line.

    ***************************************************************************/

    protected override void consoleOutput ( )
    {
        auto node_info = cast(IQueueNodeInfo)this.node;

        this.updateChannelStats();

        if ( this.stats_config.console_stats_enabled )
        {

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

        Write the metadata of the channel which will be logged then resets the
        old values.

    ***************************************************************************/

    protected override void writeLogOutput ( )
    {
        super.writeLogOutput();

        //reset channel stats
        foreach ( ref num; this.channel_stats )
        {
            num = 0;
        }
    }


    /***************************************************************************

        Update the out parameter bytes and records to the largest value of
        of the channel or what the channel had before.

        Params:
            channel = data about a channel
            bytes   = will be set to size the channel have/had
            records = will be set to numer of records the channel have/had

    ***************************************************************************/

    protected override void getChannelSize ( IStorageEngineInfo channel,
        out size_t bytes, out size_t records )
    {
        bytes = max(this.channel_stats[this.channel_bytes_title[channel.id]],
                      channel.num_bytes);

        records =max(this.channel_stats[this.channel_records_title[channel.id]],
                      channel.num_records);
    }
}
