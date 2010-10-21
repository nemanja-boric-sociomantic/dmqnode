/*******************************************************************************

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        October 2010: Initial release

    authors:        Gavin Norman

    Config file parser for dht node monitor

*******************************************************************************/

module src.core.config.MonitorConfig;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.util.Config;



/*******************************************************************************

    Dht node monitor config

*******************************************************************************/

struct MonitorConfig
{
public static:

    /***************************************************************************

        DISPLAY
    
    ***************************************************************************/

    uint columns;


    /***************************************************************************

        Reads static member variables from the config file in etc/config.ini
    
    ***************************************************************************/
    
    void init ( )
    {
        Config.init("etc/config.ini");

        // DISPLAY
        columns = Config.Int["Monitor", "columns"];
        assert(columns > 0, typeof(*this).stringof ~ ".init - not possible to display with 0 columns!");
    }
}

