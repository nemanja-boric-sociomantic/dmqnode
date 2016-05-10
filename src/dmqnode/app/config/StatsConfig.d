/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        05/06/2012: Initial release
                    30/05/2013: Combined dht and dmq project

    authors:        Gavin Norman, Hans Bjerkander

    Stats config class for use with ocean.util.config.ClassFiller.

*******************************************************************************/

module dmqnode.app.config.StatsConfig;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.util.log.Stats;



/*******************************************************************************

    Stats logging config values

*******************************************************************************/

public class StatsConfig: StatsLog.Config
{
    bool console_stats_enabled = false;
}

