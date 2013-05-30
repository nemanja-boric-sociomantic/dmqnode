/*******************************************************************************

    DHT Node Server Daemon

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        June 2009:    Initial release
                    January 2011: Asynchronous dht node

    authors:        David Eckardt, Gavin Norman
                    Thomas Nicolai, Lars Kirchhoff

    TODO: this module is extremely similar to the equivalent in the QueueNode
    project. Find a central place to combine them. Another advantage of making
    a kind of node base class would be that it'd ease the splitting of the
    logfiles & memory dht nodes into separate projects, which would enable their
    config files to be split cleanly. This would be nice.

*******************************************************************************/

module src.mod.DhtNodeServer;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.main.Version;

private import src.core.config.ServerConfig;

private import src.core.model.IDhtNode;

private import src.mod.memory.MemoryDhtNode;

private import src.mod.logfiles.LogfilesDhtNode;

private import ocean.util.app.LoggedCliApp;
private import ocean.util.app.ext.VersionArgsExt;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("src.mod.node.DhtNode");
}



/*******************************************************************************

    DhtNode

*******************************************************************************/

public class DhtNodeServer : LoggedCliApp
{
    /***************************************************************************

        Version information extension.

    ***************************************************************************/

    public VersionArgsExt ver_ext;


    /***************************************************************************

        Dht node instance

    ***************************************************************************/

    private IDhtNode node;


    /***************************************************************************

        Constructor

    ***************************************************************************/

    public this ( )
    {
        const app_name = "dhtnode";
        const app_desc = "dhtnode: distributed hashtable server node.";
        const usage = null;
        const help = null;
        const use_insert_appender = false;
        const loose_config_parsing = false;
        const char[][] default_configs = [ "etc/config.ini" ];

        super(app_name, app_desc, usage, help, use_insert_appender,
                loose_config_parsing, default_configs, config);

        this.ver_ext = new VersionArgsExt(Version);
        this.args_ext.registerExtension(this.ver_ext);
        this.log_ext.registerExtension(this.ver_ext);
        this.registerExtension(this.ver_ext);
    }


    /***************************************************************************

        Get values from the configuration file.

    ***************************************************************************/

    public override void processConfig ( IApplication app, ConfigParser config )
    {
        ServerConfig server_config;
        ConfigReader.fill("Server", server_config, config);

        log.info("Starting dht node --------------------------------");

        switch ( cast(char[])server_config.storage_engine )
        {
            case "memory":
                this.node = new MemoryDhtNode(server_config, config);
                break;

            case "logfiles":
                this.node = new LogfilesDhtNode(server_config, config);
                break;

            default:
                throw new Exception("Invalid / unsupported data storage");
        }
    }


    /***************************************************************************

        Do the actual application work. Called by the super class.

        Params:
            args = command line arguments
            config = parser instance with the parsed configuration

        Returns:
            status code to return to the OS

    ***************************************************************************/

    protected int run ( Arguments args, ConfigParser config )
    {
        this.node.run();

        return 0;
    }
}

