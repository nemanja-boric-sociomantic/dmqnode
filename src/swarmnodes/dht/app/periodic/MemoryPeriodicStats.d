/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        05/06/2012: Initial release
                    30/05/2013: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

    Overrides the standard periodic stats, adding additional information about
    the state of the memory channels dumper.

*******************************************************************************/

module swarmnodes.dht.app.periodic.MemoryPeriodicStats;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.common.kvstore.app.periodic.PeriodicKVStats;

private import swarmnodes.dht.app.periodic.ChannelDumpThread;

private import swarmnodes.common.config.StatsConfig;

private import ocean.text.convert.Layout;

private import ocean.text.util.Time;



/*******************************************************************************

    Memory node periodic stats class.

*******************************************************************************/

public class MemoryPeriodicStats : PeriodicKVStats
{
    /***************************************************************************

        Buffers used for string formatting.

    ***************************************************************************/

    private char[] buf, time_buf;


    /***************************************************************************

        Interface to the channel dumper thread (may be null).

    ***************************************************************************/

    private const IChannelDumpInfo channel_dumper;


    /***************************************************************************

        Constructor.

        Params:
            stats_config = stats log configuration (passed to super class)
            epoll = epoll select dispatcher to register this periodic with (the
                registration of periodics is usually dealt with by the Periodics
                class, but an individual periodic can also reregister itself
                with epoll in the situation where an error occurs)
            channel_dumper = interface to the channel dumper thread

    ***************************************************************************/

    public this ( StatsConfig stats_config, EpollSelectDispatcher epoll,
        IChannelDumpInfo channel_dumper )
    {
        this.channel_dumper = channel_dumper;

        super(stats_config, epoll);
    }


    /***************************************************************************

        Provides additional text to be displayed on the console stats line.
        Displays info about the status of the channel dump thread (if enabled).

        Returns:
            text to append to console stats line

    ***************************************************************************/

    override protected char[] extraConsoleInfo ( )
    {
        if ( !this.channel_dumper )
        {
            return " (no dump thread)";
        }

        if ( this.channel_dumper.busy )
        {
            return " (dumping channels)";
        }
        else
        {
            formatDurationShort(this.channel_dumper.seconds_until_dump,
                this.time_buf);

            this.buf.length = 0;
            Layout!(char).print(this.buf, " ({} until channel dump)",
                this.time_buf);

            return this.buf;
        }
    }
}

