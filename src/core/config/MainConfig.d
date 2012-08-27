/*******************************************************************************

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        November 2010: Initial release
    
    authors:        Gavin Norman

*******************************************************************************/

module src.core.config.MainConfig;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.util.config.ConfigParser;
private import ConfigReader = ocean.util.config.ClassFiller;



/*******************************************************************************

    Server config values

*******************************************************************************/

private class ServerConfig
{
    ConfigReader.Required!(char[]) address;

    ConfigReader.Required!(ushort) port;

    ulong size_limit = 0; // 0 := no global size limit

    ConfigReader.Required!(ConfigReader.Min!(ulong, 1)) channel_size_limit;

    char[] data_dir = "data";

    uint connection_limit = 5000;
}


/*******************************************************************************

    Stats logging config values

*******************************************************************************/

private class StatsConfig
{
    char[] logfile = "log/stats.log";
    bool console_stats_enabled = false;
}


/*******************************************************************************

    Config file reader

*******************************************************************************/

public class MainConfig
{
static:

    /***************************************************************************

        Instances of each config class to be read.

    ***************************************************************************/

    public ServerConfig server;
    public StatsConfig stats;


    /**************************************************************************

        Initializes configuration.

        Params:
            config = config parser instance

     **************************************************************************/

    public void init ( ConfigParser config )
    {
        ConfigReader.fill("Server", server);
        ConfigReader.fill("Stats", stats);
    }
}

