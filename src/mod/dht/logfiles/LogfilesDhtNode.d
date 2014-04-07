/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        04/06/2012: Initial release
                    30/05/2013: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

    Logfiles dht node

*******************************************************************************/

module src.mod.dht.logfiles.LogfilesDhtNode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.dht.core.model.IDhtNode;

private import src.mod.dht.core.periodic.PeriodicDhtStats;

private import src.mod.dht.storage.LogFilesStorageChannels;

private import src.mod.dht.storage.logfiles.LogRecordPut;

private import ConfigReader = ocean.util.config.ClassFiller;



/*******************************************************************************

    Logfiles node config values

*******************************************************************************/

private class LogfilesConfig
{
    size_t write_buffer_size = LogRecordPut.DefaultBufferSize;
}



/*******************************************************************************

    Logfiles node class

*******************************************************************************/

public class LogfilesDhtNode : IDhtNode
{
    /***************************************************************************

        Logfiles node specific config values

    ***************************************************************************/

    private LogfilesConfig logfiles_config;


    /***************************************************************************

        Constructor.

        Params:
            server_config = parsed server config instance
            config = config parser (used to parse logfiles node config values)

    ***************************************************************************/

    public this ( ServerConfig server_config, ConfigParser config )
    {
        ConfigReader.fill("Options_LogFiles", this.logfiles_config, config);

        super(server_config, config);

        this.periodics.add(new PeriodicDhtStats(this.stats_config, this.epoll));
    }


    /***************************************************************************

        Returns:
            a new logfiles storage channels instance.

    ***************************************************************************/

    protected DhtStorageChannels newStorageChannels_ ( )
    {
        LogFilesStorageChannels.Args args;
        args.write_buffer_size = this.logfiles_config.write_buffer_size;

        return new LogFilesStorageChannels(this.server_config.data_dir,
            0, args); // TODO: the size_limit of 0 is ignored by the logfiles
                      // storage channels -- remove this parameter altogether
    }
}

