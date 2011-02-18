/*******************************************************************************

    DHT node import

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman

    Reads records from a file and puts them to a dht.

    Command line parameters:
        -D = dhtnodes.xml file for destination dht
        -f = name of file to read records from

    Inherited from super class:
        -h = display help

*******************************************************************************/

module mod.importer.DhtImport;



/*******************************************************************************

    Imports

*******************************************************************************/

private import mod.model.DhtTool;

private import core.dht.DestinationQueue;

private import ocean.io.serialize.SimpleSerializer;

private import ocean.text.Arguments;

private import ocean.util.log.PeriodicTrace;

private import swarm.dht2.DhtClient,
               swarm.dht2.DhtHash,
               swarm.dht2.DhtConst;

private import tango.io.Stdout;

private import tango.io.device.File;



/*******************************************************************************

    Dht import tool

*******************************************************************************/

class DhtImport : DhtTool
{
    /***************************************************************************

        Singleton parseArgs() and run() methods.
    
    ***************************************************************************/

    mixin SingletonMethods;


    /***************************************************************************

        Name of file to read records from.
    
    ***************************************************************************/

    private char[] file_name;


    /***************************************************************************

        Queue of records being pushed in batches to the dht.
    
    ***************************************************************************/

    private DestinationQueue put_queue;


    /***************************************************************************

        Record key and value read from source file.
    
    ***************************************************************************/

    private char[] key, value;


    /***************************************************************************

        Main process method. Runs the tool based on the passed command line
        arguments.

        Params:
            dht = dht client to use

    ***************************************************************************/

    protected void process_ ( DhtClient dht )
    {
        this.initDestinationQueue(dht);

        scope file = new File;
        try
        {
            file.open(this.file_name, File.ReadExisting);
        }
        catch ( Exception e )
        {
            Stderr.formatln("{}.process_ - error opening source file - {}", typeof(this).stringof, e.msg);
            return;
        }
        scope ( exit ) file.close;

        this.importRecords(file, dht);
    }

    
    /***************************************************************************

        Called at the end of processing (in the super.process() method). Makes
        sure that all records are actually written to the dht.

        Params:
            dht = dht client

    ***************************************************************************/
    
    override protected void finished ( DhtClient dht )
    {
        this.put_queue.flush();
    }


    /***************************************************************************

        Sets up the list of handled command line arguments. This method sets up
        only the base class' arguments (see module header), then calls the
        addArgs_() method to set up any additional command line arguments
        required by the derived class.

        Params:
            args = arguments object

    ***************************************************************************/

    override protected void addArgs_ ( Arguments args )
    {
        args("dest").params(1).required().aliased('D').help("path of dhtnodes.xml file defining nodes to import records to");
        args("file").params(1).required().aliased('f').help("name of file (created by dhtdump) to read records from");
    }


    /***************************************************************************

        Checks whether the parsed command line args are valid.

        Params:
            args = command line arguments object to validate
    
        Returns:
            true if args are valid
    
    ***************************************************************************/
    
    override protected bool validArgs ( Arguments args )
    {
        if ( !args.exists("dest") )
        {
            Stderr.formatln("No xml destination file specified (use -D)");
            return false;
        }

        if ( !args.exists("file") )
        {
            Stderr.formatln("No record source file specified (use -f)");
            return false;
        }

        return true;
    }


    /***************************************************************************

        Reads the tool's settings from validated command line arguments.
    
        Params:
            args = arguments object to read
    
    ***************************************************************************/
    
    override protected void readArgs ( Arguments args )
    {
        super.dht_nodes_config = args.getString("dest");
        this.file_name = args.getString("file");
    }


    /***************************************************************************

        Initialises the dht destination queue.
    
        Params:
            dht = dht client to use for writing
    
    ***************************************************************************/
    
    private void initDestinationQueue ( DhtClient dht )
    {
        this.put_queue = new DestinationQueue(dht);
    
        this.put_queue.setChannel(this.file_name);
    }


    /***************************************************************************

        Reads records from a file and pushes them to a dht, displaying a count
        of records imported.

        Params:
            file = file to read records from
            dht = dht client to use for writing

    ***************************************************************************/

    private void importRecords ( File file, DhtClient dht )
    {
        ulong record_count;
        bool end_of_file;
        do
        {
            try
            {
                SimpleSerializer.read(file, this.key);
                SimpleSerializer.read(file, this.value);

                StaticPeriodicTrace.format("{} records", ++record_count);

                this.put_queue.put(DhtHash.straightToHash(this.key), this.value);
            }
            catch ( Exception e )
            {
                end_of_file = true;
            }
        }
        while ( !end_of_file );

        Stdout.formatln("Finished, imported {} records", record_count);
    }
}

