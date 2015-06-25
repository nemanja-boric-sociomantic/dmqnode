/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        04/09/2012: Initial release
                    30/05/2013: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

    Performance config class for use with ocean.util.config.ClassFiller.

*******************************************************************************/

module dmqnode.app.config.PerformanceConfig;



/*******************************************************************************

    Performance config values

*******************************************************************************/

public class PerformanceConfig
{
    uint write_flush_ms = 250;
}

