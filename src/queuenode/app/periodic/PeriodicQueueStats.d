/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012: Initial release
                    30/05/2013: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

    Periodic update of stats display on the console and lines written to the
    stats log.

*******************************************************************************/

module queuenode.app.periodic.PeriodicQueueStats;



/*******************************************************************************

    Imports

*******************************************************************************/

private import queuenode.app.periodic.PeriodicStats;

private import queuenode.app.config.StatsConfig;

private import queuenode.node.IQueueNodeInfo;

private import swarm.core.node.storage.model.IStorageEngineInfo;

private import ocean.core.Array : copy, concat;

private import ocean.math.SlidingAverage;

private import ocean.text.util.DigitGrouping;

private import ocean.text.convert.Layout;

private import ocean.util.log.Stats;

private import ocean.util.log.StaticTrace;

private import tango.core.Memory;

private import tango.math.Math : max;


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
            epoll = epoll select dispatcher to register this periodic with (the
                registration of periodics is usually dealt with by the Periodics
                class, but an individual periodic can also reregister itself
                with epoll in the situation where an error occurs)

    ***************************************************************************/

    public this ( StatsConfig stats_config, EpollSelectDispatcher epoll )
    {
        super(stats_config, epoll, typeof(this).stringof);
    }


    /***************************************************************************

        Writes the provided fields to the console output line, along with queue
        specific information.

        Params:
            memory_buf = information about the memory usage of the app, or an
                empty string if not built with -version=CDGC
            records_buf = the number of records in the node
            bytes_buf = the number of bytes in the node
            rec_per_sec = the number of records handled by the node  per second

    ***************************************************************************/

    protected override void writeConsoleOutput ( char[] memory_buf,
        char[] records_buf, char[] bytes_buf, real rec_per_sec )
    {
        auto node_info = cast(IQueueNodeInfo)this.node;

        StaticTrace.format("  {} queue {}: {} conns, {} rec/s, {} recs ({})",
            node_info.storage_type, memory_buf, node_info.num_open_connections,
            rec_per_sec, records_buf, bytes_buf).flush;
    }


    /***************************************************************************

        Write the metadata of the channel which will be logged then resets the
        old values. The old values are reset here by the queue node so that the
        stats which are stored are the maximum values since the last log output.
        See also getChannelSize(), below.

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
