/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        05/06/2012: Initial release

    authors:        Gavin Norman

    Overrides the standard periodic stats, adding additional information about
    the state of the memory channels dumper.

*******************************************************************************/

module src.mod.memory.MemoryPeriodicStats;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.core.periodic.PeriodicStats;

private import src.mod.memory.ChannelDumpThread;

private import src.core.config.StatsConfig;

private import ocean.text.convert.Layout;

private import ocean.text.util.Time;



/*******************************************************************************

    Memory node periodic stats class.

*******************************************************************************/

public class MemoryPeriodicStats : PeriodicStats
{
    /***************************************************************************

        Buffers used for string formatting.

    ***************************************************************************/

    private char[] buf, time_buf;


    /***************************************************************************

        Interface to the channel dumper thread.

    ***************************************************************************/

    private const IChannelDumpInfo channel_dumper;


    /***************************************************************************

        Constructor.

        Params:
            stats_config = stats log configuration (passed to super class)
            channel_dumper = interface to the channel dumper thread

    ***************************************************************************/

    public this ( StatsConfig stats_config, IChannelDumpInfo channel_dumper )
    {
        this.channel_dumper = channel_dumper;

        super(stats_config);
    }


    /***************************************************************************

        Provides additional text to be displayed on the console stats line.
        Displays info about the status of the channel dumper.

        Returns:
            text to append to console stats line

    ***************************************************************************/

    override protected char[] consoleOutput_ ( )
    {
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

