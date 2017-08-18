/*******************************************************************************

    Performance config class for use with ocean.util.config.ClassFiller.

    copyright:
        Copyright (c) 2012-2017 sociomantic labs GmbH. All rights reserved

*******************************************************************************/

module dmqnode.app.config.PerformanceConfig;



/*******************************************************************************

    Performance config values

*******************************************************************************/

public class PerformanceConfig
{
    /// For non-neo requests: flush write buffers with this period.
    uint write_flush_ms = 250;

    /// For neo connections: toggles Nagle's algorithm (true = disabled, false =
    /// enabled) on the underlying socket.
    bool no_delay;
}

