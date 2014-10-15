/*******************************************************************************

    Logfiles Node Server

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        June 2009:    Initial release
                    January 2011: Asynchronous dht node
                    May 2013: Combined dht and queue project

    authors:        David Eckardt, Gavin Norman
                    Thomas Nicolai, Lars Kirchhoff
                    Hans Bjerkander

*******************************************************************************/

module swarmnodes.logfiles.main;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.common.kvstore.app.IKVNodeApp;

private import swarmnodes.common.kvstore.app.periodic.PeriodicKVStats;

private import swarmnodes.logfiles.storage.LogFilesStorageChannels;

private import swarmnodes.logfiles.storage.LogRecordPut;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("swarmnodes.logfiles.main");
}



/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts dht node.

    Params:
        cl_args = array with raw command line arguments

*******************************************************************************/

private int main ( char[][] cl_args )
{
    auto app = new LogfilesNodeServer;
    return app.main(cl_args);
}



/*******************************************************************************

    KVNode

*******************************************************************************/

public class LogfilesNodeServer : IKVNodeApp
{
    /***************************************************************************

        Logfiles node config values

    ***************************************************************************/

    private static class LogfilesConfig
    {
        size_t write_buffer_size = LogRecordPut.DefaultBufferSize;
    }

    private LogfilesConfig logfiles_config;


    /***************************************************************************

        Get values from the configuration file. Overridden to read additional
        logfiles config options.

        Params:
            app = application instance
            config = config parser instance

    ***************************************************************************/

    override protected void processConfig ( IApplication app, ConfigParser config )
    {
        super.processConfig(app, config);

        ConfigReader.fill("Options_LogFiles", this.logfiles_config, config);
    }


    /***************************************************************************

        Returns:
            a new logfiles storage channels instance.

    ***************************************************************************/

    override protected KVStorageChannels newStorageChannels_ ( )
    {
        return new LogFilesStorageChannels(this.server_config.data_dir, 0,
            this.hash_range, this.logfiles_config.write_buffer_size);
            // TODO: the size_limit of 0 is ignored by the logfiles storage
            // channels -- remove this parameter altogether
    }


    /***************************************************************************

        Sets up any periodics required by the node. Calls the super class'
        method and sets up the stats periodic.

        Params:
            periodics = periodics instance to which periodics can be added

    ***************************************************************************/

    override protected void initPeriodics ( Periodics periodics )
    {
        super.initPeriodics(periodics);

        periodics.add(new PeriodicKVStats(this.stats_config, this.epoll));
    }
}

