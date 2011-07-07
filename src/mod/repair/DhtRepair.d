/*******************************************************************************

    DHT node repair

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        March 2011: Initial release

    authors:        Gavin Norman

    Scans dht data files for errors, optionally fixes found errors.

    Command line parameters:
        -h = display help
        -c = channel name to repair
        -s = start of range to process (hash value - defaults to 0x00000000)
        -e = end of range to process (hash value - defaults to 0xFFFFFFFF)
        -r = repairs any problems found during scanning

    TODO: Memory node support. For memory nodes it is essential to check if the
    node is running and refuse to repair if so. The node's ip & port can be
    found out by looking at the config file, then try to query it to see it it's
    running.

*******************************************************************************/

module src.mod.repair.DhtRepair;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.repair.storagerepair.model.IStorageRepair;

private import src.mod.repair.storagerepair.LogFilesRepair;

private import src.mod.model.DhtTool;

private import ocean.util.Config;

private import ocean.text.Arguments;

private import swarm.dht.DhtConst;

private import tango.io.Stdout;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Dht repair tool

*******************************************************************************/

public class DhtRepair
{
    /***************************************************************************

        Singleton parseArgs() and run() methods.

    ***************************************************************************/

    mixin DhtTool.SingletonMethods;


    /***************************************************************************

        Alias for storage engine type

    ***************************************************************************/

    private alias DhtConst.Storage.BaseType Storage;


    /***************************************************************************

        Processing mode
    
    ***************************************************************************/

    private IStorageRepair.Mode mode;


    /***************************************************************************

        Hash range to process
    
    ***************************************************************************/
    
    private hash_t start, end;


    /***************************************************************************

        Parses and validates command line arguments using the passed Arguments
        object. The list of valid arguments  is set in the addArgs() method.

        Params:
            args = arguments object used to parse command line arguments
            arguments = list of command line arguments (excluding the executable
                name)

        Returns:
            true if the command line args are valid

    ***************************************************************************/

    public bool validateArgs ( Arguments args, char[][] arguments )
    {
        this.addArgs(args);

        if ( arguments.length && !args.parse(arguments) )
        {
            Stderr.formatln("Invalid arguments");
            return false;
        }

        return this.validArgs(args);
    }


    /***************************************************************************

        Main process method. Runs the tool based on the passed command line
        arguments.

        Params:
            args = arguments object

    ***************************************************************************/

    public void process ( Arguments args )
    in
    {
        assert(this.validArgs(args), typeof(this).stringof ~ "process - invalid arguments");
    }
    body
    {
        this.readArgs(args);

        IStorageRepair repair;

        auto node_type = this.determineNodeType();
        switch ( node_type )
        {
            case DhtConst.Storage.LogFiles:
                repair = new LogFilesRepair;
            break;

//            case DhtConst.Storage.Memory:
// TODO: check that node is not running before repairing!
//            break;

            default:
                Stderr.formatln("This node uses an unsupported storage engine: {}", DhtConst.Storage.description(node_type));
                return;
        }

        repair.process(args.getString("channel"), this.start, this.end, this.mode);
    }


    /***************************************************************************

        Reads the dht config.ini file to determine which type of node is
        running.

        Returns:
            storage engine identifier

    ***************************************************************************/

    private Storage determineNodeType ( )
    {
        switch ( Config.get!(char[])("Server", "storage_engine") )
        {
            case "hashtable":
                return DhtConst.Storage.HashTable;

            case "btree":
                return DhtConst.Storage.Btree;
        
            case "filesystem":
                return DhtConst.Storage.FileSystem;
        
            case "memory":
                return DhtConst.Storage.Memory;
        
            case "logfiles":
                return DhtConst.Storage.LogFiles;

            default:
        }

        return DhtConst.Storage.None;
    }


    /***************************************************************************

        Sets up the list of handled command line arguments.

        Params:
            args = arguments object

    ***************************************************************************/

    private void addArgs ( Arguments args )
    {
        args("help").aliased('?').aliased('h').help("display this help");
        args("channel").aliased('c').required().params(1).help("channel name to repair");
        args("start").aliased('s').params(1).help("start of range to process (hash value - defaults to 0x00000000)");
        args("end").aliased('e').params(1).help("end of range to process (hash value - defaults to 0xFFFFFFFF)");
        args("repair").aliased('r').help("repairs any problems found during scanning");
    }


    /***************************************************************************

        Validates command line arguments in the passed Arguments object.

        Params:
            args = arguments object used to parse command line arguments

        Returns:
            true if the command line args are valid

    ***************************************************************************/

    private bool validArgs ( Arguments args )
    {
        if ( !args.exists("channel") )
        {
            Stderr.formatln("Please specify which channel to repair (use -c)");
            return false;
        }

        return true;
    }


    /***************************************************************************

        Reads the tool's settings from validated command line arguments.

        Params:
            args = arguments object

    ***************************************************************************/

    private void readArgs ( Arguments args )
    {
        this.start = args.exists("start") ? args.getInt!(hash_t)("start") : 0x00000000;
        this.end = args.exists("end") ? args.getInt!(hash_t)("end") : 0xffffffff;

        this.mode = args.exists("repair") ? this.mode.Repair : this.mode.Check;
    }
}

