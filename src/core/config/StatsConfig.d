/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        05/06/2012: Initial release

    authors:        Gavin Norman

    Stats config class for use with ocean.util.config.ClassFiller.

*******************************************************************************/

module src.core.config.StatsConfig;



/*******************************************************************************

    Stats logging config values

*******************************************************************************/

public class StatsConfig
{
    char[] logfile = "log/stats.log";
    bool console_stats_enabled = false;
}

