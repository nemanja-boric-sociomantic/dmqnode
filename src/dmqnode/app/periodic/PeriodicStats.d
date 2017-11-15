/*******************************************************************************

    copyright:
        Copyright (c) 2012-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.app.periodic.PeriodicStats;


import dmqnode.app.config.StatsConfig;
import dmqnode.storage.Ring;
import dmqnode.node.IDmqNodeInfo;

import swarm.util.node.log.Stats;

import ocean.util.log.Stats;
import ocean.util.log.Log;


/*******************************************************************************

    Periodic stats class. Displays a message line to the console and writes
    lines to a stats log.

*******************************************************************************/

public class PeriodicStats
{
    /***************************************************************************

        Definition of the per-channel statistics to log in addition to those
        automatically written by ChannelsNodeStats.

    ***************************************************************************/

    struct ChannelStats
    {
        /***********************************************************************

            The number of bytes stored in the memory queue.

        ***********************************************************************/

        ulong bytes_memory;

        /***********************************************************************

            The number of bytes stored in the overflow.

        ***********************************************************************/

        ulong bytes_overflow;

        /***********************************************************************

            The number of records stored in the memory queue.

        ***********************************************************************/

        uint  records_memory;

        /***********************************************************************

            The number of records stored in the overflow.

        ***********************************************************************/

        uint  records_overflow;

        /***********************************************************************

            The relative fullness of the memory queue in percent.

        ***********************************************************************/

        ubyte percent_memory;


        /***********************************************************************

            Creates a new instance of this struct, populated with the
            corresponding stats counter values from node.

            Params:
                node = node to get statistics from

            Returns:
                an instance of this struct populated with the correspondent
                values in node.

        ***********************************************************************/

        static typeof(*this) set ( RingNode.Ring channel )
        {
            auto stats = typeof(*this)(channel.memory_info.used_space,
                                       channel.overflow_info.num_bytes,
                                       cast(uint)channel.memory_info.length,
                                       channel.overflow_info.num_records,
                                       100);
            if (auto mem_capacity = channel.memory_info.total_space)
            {
                /*
                 * channel.memory_info.total_space == 0 would be a memory queue
                 * of zero capacity, which should be impossible in production.
                 * Still it's a good idea to prevent a division by 0 as this may
                 * happen in special test builds.
                 */
                stats.percent_memory = cast(ubyte)((stats.bytes_memory * 100.) / mem_capacity);
            }

            return stats;
        }
    }

    /***************************************************************************

        Interface to the DMQ node.

    ***************************************************************************/

    protected IDmqNodeInfo node_info;

    /***************************************************************************

        Stats config object, passed into constructor.

    ***************************************************************************/

    protected StatsConfig stats_config;

    /***************************************************************************

        Log file

    ***************************************************************************/

    protected StatsLog log;

    /***************************************************************************

        Logger for the node and basic per-channel stats. The additional
        per-channel stats in ChannelStats are logged separately.

    ***************************************************************************/

    private ChannelsNodeStats basic_channel_stats;

    /***************************************************************************

        The number of seconds since the log was written the last time.

        This member is for console output only, remove it  when making the
        application a demon.

    ***************************************************************************/

    private uint seconds = 0;

    /***************************************************************************

        Constructor.

        Params:
            node_info = DMQ node info
            stats_config = class containing configuration settings for stats

    ***************************************************************************/

    public this ( IDmqNodeInfo node_info, StatsConfig stats_config )
    {
        this.node_info = node_info;

        this.stats_config = stats_config;

        this.log = new StatsLog(stats_config);

        this.basic_channel_stats = new ChannelsNodeStats(node_info, this.log);
    }

    /***************************************************************************

        Should be called once every second by a timer. Writes a line to the
        stats log if the write period (30s as specified by
        StatsLog.default_period) has passed.

        Returns:
            always true to stay registered with TimerExt

    ***************************************************************************/

    public bool run ( )
    {
        // When removing the console log output, call this method every
        // this.log.default_period * 1000 seconds.
        if (++this.seconds >= this.log.default_period)
        {
            this.seconds = 0;
            this.writeStatsLog();
        }

        return true;
    }

    /***************************************************************************

        Writes the stats log.

        When removing the console log, replace run() with this method.

    ***************************************************************************/

    private void writeStatsLog ( )
    {
        this.basic_channel_stats.log();

        foreach (channel; this.node_info)
        {
            this.log.addObject!("channel")(channel.id, ChannelStats.set(channel));
        }

        this.log.add(Log.stats);
        this.log.flush();
        this.node_info.resetCounters();
    }
}
