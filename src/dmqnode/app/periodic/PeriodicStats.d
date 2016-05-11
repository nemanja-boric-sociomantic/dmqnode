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

        Stats config object, passed into constructor.

    ***************************************************************************/

    protected const StatsConfig stats_config;

    /***************************************************************************

        Log file

    ***************************************************************************/

    protected StatsLog log;

    /***************************************************************************

        The number of seconds since the log was written the last time.

        This member is for console output only, remove it  when making the
        application a demon.

    ***************************************************************************/

    private uint seconds = 0;

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
        // When removing the console log output, multiply the 1000 with
        // this.log.default_period.
        super(epoll, 1000, typeof(this).stringof);

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
        this.consoleLog();

        if (++this.seconds >= this.log.default_period)
        {
            this.seconds = 0;
            this.writeStatsLog();
        }
    }

    /***************************************************************************

        Writes the stats log.

        When removing the console log, replace run() with this method.

    ***************************************************************************/

    private void writeStatsLog ( )
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
        this.node_info.resetCounters();
    }

    /***************************************************************************

        Temporary, will be removed when making this application a demon.

    ***************************************************************************/

    import tango.stdc.stdio;

    void consoleLog ( )
    {
        if (this.stats_config.console_stats_enabled)
        {
            uint i = 0;

            foreach (channel; this.node_info)
            {
                char[] id = channel.id;
                fwrite(id.ptr, id[0].sizeof, id.length, stderr);
                fputs(" records: ".ptr, stderr);
                this.printRecords(cast(uint)channel.memory_info.length);
                fputs(" mem ".ptr, stderr);
                this.printRecords(channel.overflow_info.num_records);
                fputs(" ovf".ptr, stderr);
                if (auto n = channel.records_pushed)
                {
                    fputs(" +".ptr, stderr);
                    this.printRecords(n);
                    channel.records_pushed = 0;
                }

                if (auto n = channel.records_popped)
                {
                    fputs(" -".ptr, stderr);
                    this.printRecords(n);
                    channel.records_popped = 0;
                }

                fputs("; bytes: ".ptr, stderr);
                this.printBytes(channel.memory_info.used_space);
                fputs(" mem ".ptr, stderr);
                this.printBytes(channel.overflow_info.num_bytes);
                fputs(" ovf".ptr, stderr);
                if (auto n = channel.bytes_pushed)
                {
                    fputs(" +".ptr, stderr);
                    this.printBytes(n);
                    channel.bytes_pushed = 0;
                }

                if (auto n = channel.bytes_popped)
                {
                    fputs(" -".ptr, stderr);
                    this.printBytes(n);
                    channel.bytes_popped = 0;
                }

                fputs("\x1B[K\n".ptr, stderr);

                i++;
            }

            if (i) fprintf(stderr, "\x1B[%uA\r".ptr, i);
        }
    }

    /// Prints b to stderr using 1024 based digit grouping.

    static void printBytes ( size_t b )
    {
        if (b)
        {
            ushort[7] d;

            uint n = 0;

            while (b)
            {
                d[n++] = cast(ubyte)(b & ((1 << 10) - 1));
                b >>= 10;
            }

            fprintf(stderr, "%hu".ptr, d[--n]);

            foreach_reverse (digit; d[0 .. n])
            {
                fprintf(stderr, ",%03hu".ptr, digit);
            }
        }
        else
        {
            fputc('0', stderr);
        }
    }

    /// Prints b to stderr using 1000 based digit grouping.

    static void printRecords ( uint r )
    {
        if (r)
        {
            ushort[10] d;

            uint n = 0;

            while (r)
            {
                d[n++] = cast(ushort)(r % 1000);
                r /= 1000;
            }

            fprintf(stderr, "%hu".ptr, d[--n]);

            foreach_reverse (digit; d[0 .. n])
            {
                fprintf(stderr, ",%03hu".ptr, digit);
            }
        }
        else
        {
            fputc('0', stderr);
        }
    }
}

