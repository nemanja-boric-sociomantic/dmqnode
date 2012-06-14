/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        04/06/2012: Initial release

    authors:        Gavin Norman

    Logfiles dht node

*******************************************************************************/

module src.mod.node.logfiles.LogfilesDhtNode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.node.model.IDhtNode;

private import src.mod.node.periodic.PeriodicStats;

private import swarm.dht.node.storage.LogFilesStorageChannels;

private import ConfigReader = ocean.util.config.ClassFiller;

private import swarm.dht.node.storage.filesystem.LogRecordPut;



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
        super(server_config, config);

        this.periodics.add(new PeriodicStats(this.stats_config));

        ConfigReader.fill("Options_LogFiles", this.logfiles_config, config);
    }


    /***************************************************************************

        Returns:
            a new logfiles storage channels instance.

    ***************************************************************************/

    protected StorageChannels newStorageChannels_ ( )
    {
        LogFilesStorageChannels.Args args;
        args.write_buffer_size = this.logfiles_config.write_buffer_size;

        return new LogFilesStorageChannels(this.server_config.data_dir,
            0, args); // TODO: the size_limit of 0 is ignored by the logfiles
                      // storage channels -- remove this parameter altogether
    }
}

