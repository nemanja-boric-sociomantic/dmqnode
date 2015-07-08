/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012 2012: Initial release
                    30/05/2013: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

*******************************************************************************/

module dmqnode.app.periodic.PeriodicStats;



/*******************************************************************************

    Imports

*******************************************************************************/

private import dmqnode.app.periodic.model.IPeriodic;

private import dmqnode.app.config.StatsConfig;

private import dmqnode.storage.Ring;

private import ocean.util.log.Stats;

/*******************************************************************************

    Periodic stats class. Displays a message line to the console and writes
    lines to a stats log. Contains shared methods which both the dht and the
    queue can use.

*******************************************************************************/

public class PeriodicStats : IPeriodic
{
    /***************************************************************************

        Definition of the global statistics to log.

    ***************************************************************************/

    struct NodeStats
    {
        /***********************************************************************

            The number of open socket connections to clients.

        ***********************************************************************/

        uint handling_connections;

        /***********************************************************************

            The total number of bytes received through the client connections.

        ***********************************************************************/

        ulong bytes_received;

        /***********************************************************************

            The total number of bytes sent through the client connections.

        ***********************************************************************/

        ulong bytes_sent;

        /***********************************************************************

            Creates a new instance of this struct, populated with the
            corresponding stats counter values from node.

            Params:
                node = node to get statistics from

            Returns:
                an instance of this struct populated with the correspondent
                values in node.

        ***********************************************************************/

        static typeof(*this) set ( IDmqNodeInfo node )
        {
            return typeof(*this)(node.num_open_connections,
                                 node.bytes_received,
                                 node.bytes_sent);
        }
    }

    /***************************************************************************

        Definition of the per-channel statistics to log.

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
                                       channel.memory_info.length,
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

        Stats config object, passed into constructor.

    ***************************************************************************/

    protected const StatsConfig stats_config;

    /***************************************************************************

        Log file

    ***************************************************************************/

    protected StatsLog log;

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
        super(epoll, this.log.default_period * 1000, typeof(this).stringof);

        this.stats_config = stats_config;

        this.log = new StatsLog(this.stats_config.file_count,
            this.stats_config.max_file_size, this.stats_config.logfile);
    }

    /***************************************************************************

        Called once every second by the base class. Updates the console display,
        and write a line to the stats log if the write period (30s as specified
        by StatsLog.default_period) has passed.

    ***************************************************************************/

    override protected void run ( )
    {
        this.log.add(NodeStats.set(this.node_info));

        foreach (action_name, action_stats; this.node_info.record_action_counters)
        {
            this.log.addObject!("action")(action_name, action_stats);
        }

        foreach (channel; this.node_info)
        {
            this.log.addObject!("channel")(channel.id, ChannelStats.set(channel));
        }

        this.log.flush();
    }
}

