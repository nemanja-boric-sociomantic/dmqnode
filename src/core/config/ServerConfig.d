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

    ConfigReader.Required!(char[]) minval;

    ConfigReader.Required!(char[]) maxval;

    ConfigReader.Required!(char[]) storage_engine;

    char[] data_dir = "data";
}
