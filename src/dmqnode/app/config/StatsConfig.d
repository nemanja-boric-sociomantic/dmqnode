/*******************************************************************************

    Stats config class for use with ocean.util.config.ClassFiller.

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

*******************************************************************************/

module dmqnode.app.config.StatsConfig;


import ocean.util.log.Stats;

/*******************************************************************************

    Stats logging config values

*******************************************************************************/

public class StatsConfig: StatsLog.Config
{
    bool console_stats_enabled = false;
}
