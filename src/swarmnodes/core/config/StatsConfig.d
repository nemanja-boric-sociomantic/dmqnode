/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        05/06/2012: Initial release
                    30/05/2013: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

    Stats config class for use with ocean.util.config.ClassFiller.

*******************************************************************************/

module swarmnodes.core.config.StatsConfig;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.util.log.Stats;



/*******************************************************************************

    Stats logging config values

*******************************************************************************/

public class StatsConfig
{
    char[] logfile = IStatsLog.default_file_name;
    size_t file_count = IStatsLog.default_file_count;
    size_t max_file_size = IStatsLog.default_max_file_size;
    bool console_stats_enabled = false;
}

