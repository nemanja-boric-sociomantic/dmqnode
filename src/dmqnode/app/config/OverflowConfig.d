/*******************************************************************************

    Copyright (c) 2015 sociomantic labs. All rights reserved

    Disk overflow config class for use with ocean.util.config.ClassFiller.

*******************************************************************************/

module dmqnode.app.config.OverflowConfig;



/*******************************************************************************

    Overflow config values

*******************************************************************************/

public class OverflowConfig
{
    uint write_index_ms = 60_000;
}

