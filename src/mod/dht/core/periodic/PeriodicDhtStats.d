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

        Writes the provided fields to the console output line, along with dht
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
        auto node_info = cast(IDhtNodeInfo)this.node;

        StaticTrace.format("  {} dht 0x{:X16}..0x{:X16}{}: {} conns, {} rec/s, {} recs ({}){}",
            node_info.storage_type, node_info.min_hash, node_info.max_hash,
            memory_buf, node_info.num_open_connections, rec_per_sec,
            records_buf, bytes_buf,
            this.extraConsoleInfo()).flush;
    }


    /***************************************************************************

        Provides additional text to be displayed on the console stats line. The
        default implementation does nothing, but deriving classes can override
        in order to display additional information.

        Returns:
            text to append to console stats line

    ***************************************************************************/

    protected char[] extraConsoleInfo ( )
    {
        return "";
    }
}

