/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        04/06/2012: Initial release

    authors:        Gavin Norman

    Server config class for use with ocean.util.config.ClassFiller.

*******************************************************************************/

module src.core.config.ServerConfig;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ConfigReader = ocean.util.config.ClassFiller;



/*******************************************************************************

    Server config values

*******************************************************************************/

public class ServerConfig
{
    ConfigReader.Required!(char[]) address;

    ConfigReader.Required!(ushort) port;

    ulong size_limit = 0; // 0 := no global size limit

    ConfigReader.Required!(ConfigReader.Min!(ulong, 1)) channel_size_limit;

    char[] data_dir = "data";

    uint connection_limit = 5000;
}
