/*******************************************************************************

    Disk overflow config class for use with ocean.util.config.ClassFiller.

    copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved

*******************************************************************************/

module dmqnode.app.config.OverflowConfig;



/*******************************************************************************

    Overflow config values

*******************************************************************************/

public class OverflowConfig
{
    uint write_index_ms = 60_000;
}

