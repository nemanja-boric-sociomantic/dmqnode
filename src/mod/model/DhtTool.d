/*******************************************************************************

    DHT node tool abstract class

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        December 2010: Initial release

    authors:        Gavin Norman

    Base class for DHT tools which connect to a node cluster specified in an xml
    file.

    Provides the following command line parameters:
        -h = display help
        -x = the number of connection to use to connect to dht(s)

*******************************************************************************/

module src.mod.model.DhtTool;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.io.select.EpollSelectDispatcher;

private import ocean.text.Arguments : Arguments;

private import ocean.core.Exception : assertEx;

private import swarm.dht.DhtClient,
               swarm.dht.DhtHash,
               swarm.dht.DhtConst;

private import tango.io.Stdout;


/*******************************************************************************

    Dht tool abstract class

*******************************************************************************/

abstract class IDhtTool
{
    /***************************************************************************

        Epoll selector instance

    ***************************************************************************/

    protected EpollSelectDispatcher epoll;


    /***************************************************************************

        Number of client connections to each dht node

    ***************************************************************************/

    protected uint connections;


    /***************************************************************************

        Flag indicating whether a dht error has occurred

    ***************************************************************************/

    protected bool dht_error;


    /***************************************************************************

        Buffer for dht client message formatting.

    ***************************************************************************/

    private char[] message_buffer;


    /***************************************************************************

        Parses and validates command line arguments using the passed Arguments
        object. The list of valid arguments for the base class (see module
        header) is set in the addArgs() method. Derived classes can override the
        addArgs_() method to specify additional arguments.

        Params:
            exe_name = name of program executable
            args = arguments object used to parse command line arguments
            arguments = list of command line arguments (excluding the executable
                name)

        Returns:
            true if the command line args are valid

    ***************************************************************************/

    final public bool parseArgs ( char[] exe_name, Arguments args,
                                    char[][] arguments )
    {
        this.addArgs(args);

        if ( !args.parse(arguments) )
        {
            if ( !args.exists("help") )
            {
                Stderr.formatln("Invalid arguments:");
                args.displayErrors();
            }

            args.displayHelp();

            return false;
        }

        if ( this.validArgs(args) )
        {
            return true;
        }
        else
        {
            args.displayHelp();

            return false;
        }
    }


    /***************************************************************************

        Reads the tool's settings from validated command line arguments. The
        base class' arguments (see module header) are read by the readArgs()
        method. Derived classes should override this method to read any
        additional arguments.

        Params:
            args = arguments object to read

    ***************************************************************************/

    protected void readArgs ( Arguments args )
    in
    {
        assert(this.validArgs(args),
            typeof(this).stringof ~ ".readArgs_: invalid arguments");
    }
    body
    {
        this.connections = args.getInt!(uint)("connections");
        this.readArgs_(args);
    }

    abstract protected void readArgs_ ( Arguments args );


    /***************************************************************************

        Sets up the list of handled command line arguments. This method sets up
        only the base class' arguments (see module header), then calls the
        addArgs_() method to set up any additional command line arguments
        required by the derived class.

        Params:
            args = arguments object

    ***************************************************************************/

    protected void addArgs ( Arguments args )
    {
        args("connections").aliased('x').params(1).defaults("10").help("number of connections to each node in the dht");
        args("help").aliased('?').aliased('h').help("display this help");
        this.addArgs_(args);
    }


    /***************************************************************************

        Sets up the list of command line arguments handled by the derived class.
        The base class' arguments are set up by the addArgs() method. Derived
        classes should override this method to add any additional arguments.

        Params:
            args = arguments object

    ***************************************************************************/

    protected void addArgs_ ( Arguments args )
    {
    }


    /***************************************************************************

        Performs any additional command line argument validation which cannot be
        performed by the Arguments class. Derived classes should override
        this method to perform any validation required.

        Params:
            args = arguments object used to parse command line arguments

        Returns:
            true if the command line args are valid

    ***************************************************************************/

    protected bool validArgs ( Arguments args )
    {
        if ( args.getInt!(uint)("connections") > 0 )
        {
            Stderr.formatln("Tool cannot function with 0"
                            " connections per dht node");
            return false;
        }

        return true;
    }


    /***************************************************************************

        Main process method. Runs the tool based on the passed command line
        arguments. Reads command line args, sets up the dht client, then calls
        the abstract process_(), which must be implemented by deriving classes.
        Finally, calls the finished() method.

        Params:
            args = arguments object

    ***************************************************************************/

    final public void run ( Arguments args )
    in
    {
        assert(this.validArgs(args),
            typeof(this).stringof ~ ".process -- invalid arguments");
    }
    body
    {
        this.readArgs(args);

        this.epoll = new EpollSelectDispatcher;

        this.initDhts();

        this.init();

        this.process_();

        this.finished();
    }


    /***************************************************************************

        While the initDhtClient() method performs the actual DhtNode
        initialization, however it performs that for just a single DhtNode.
        This method on the other hand should be overrided to define how many
        DhtClients are required (i.e. you should override this method and call
        inside it initDhtClient() as many as how many Dhts you have).

    ***************************************************************************/

    abstract protected void initDhts ();


    /***************************************************************************

        Implementation dependent process method.

        Params:
            dht = dht client to use

    ***************************************************************************/

    abstract protected void process_ (  );


    /***************************************************************************

        Called before processing (in the process() method, above). The base
        class implementation does nothing, but derived classes may wish to add
        behaviour at this point.

    ***************************************************************************/

    protected void init (  )
    {
    }


    /***************************************************************************

        Called at the end of processing (in the process() method, above). The
        base class implementation does nothing, but derived classes may wish to
        add behaviour at this point.

    ***************************************************************************/

    protected void finished (  )
    {
    }


    /***************************************************************************

        Initialises a dht client, connecting to nodes in the cluster specified
        in the xml config file.

        Params:
            xml = name of xml file defining nodes in dht cluster

        Returns:
            initialised dht client

        Throws:
            asserts that no errors occurred during initialisation

    ***************************************************************************/

    protected DhtClient initDhtClient ( char[] xml, uint connections,
        uint request_queue_size = 1000 )
    {
        debug (DhtTool) Stderr.formatln(
            "Initialising dht client connections from {}", xml);

        auto dht = new DhtClient(this.epoll, connections, request_queue_size);

        dht.addNodes(xml);

        dht.nodeHandshake(
                ( DhtClient.RequestContext, bool ok )
                {
                    if ( !ok )
                    {
                        this.dht_error = true;
                    }
                }, &this.notifier);
        this.epoll.eventLoop();

        if ( this.strictHandshake )
        {
            assertEx(!this.dht_error, typeof(this).stringof ~ ".initDhtClient - "
                "error during dht client initialisation of " ~ xml);
        }

        debug (DhtTool) Stderr.formatln("Dht client connections initialised");

        return dht;
    }


    /***************************************************************************

        Dht error callback. Outputs the error message to the console and sets an
        internal flag to indicate that an error has occurred.

        Params:
            e = error info

    ***************************************************************************/

    protected void notifier ( DhtClient.RequestNotification info )
    {
        if ( info.type == info.type.Finished && !info.succeeded )
        {
            Stderr.format("DHT client error: {}\n", info.message(this.message_buffer));
            this.dht_error = true;
        }
    }


    /***************************************************************************

        Returns:
            true if the tool should fail if any errors occur during node
            handshake

    ***************************************************************************/

    protected bool strictHandshake ( )
    {
        return true;
    }


    /***************************************************************************

        Helper function to tell whether exactly one of the passed list of bools
        is true.

        Params:
            bools = variadic list of bools to check

        Returns:
            true if exactly one of the passed bools is true

    ***************************************************************************/

    static protected bool oneTrue ( bool[] bools ... )
    {
        uint true_count;
        foreach ( b; bools )
        {
            if ( b ) true_count++;
        }

        return true_count == 1;
    }
}


/***************************************************************************

    The class should be inherited when multiple dhts will be used.

***************************************************************************/

abstract class MultiDhtTool : IDhtTool
{
    /***************************************************************************

        Names of the xml files which include the dht nodes config

    ***************************************************************************/

    protected char[][] dht_nodes_config;


    /***************************************************************************

        Dht clients instances

    ***************************************************************************/

    protected DhtClient[] dhts;


    /***************************************************************************

        While the initDhtClient() method performs the actual DhtNode
        initialization, however it performs that for just a single DhtNode.
        This method on the other defines how many DhtClients are required.

    ***************************************************************************/

    override protected void initDhts ()
    {
        foreach ( config_file; dht_nodes_config)
        {
            this.dhts ~= super.initDhtClient(config_file, connections);
        }
    }


    /***************************************************************************

        Reads the tool's settings from validated command line arguments. The
        method calls the super readArgs and then parses the additional arguments
        that are not handled by the base class.

        Params:
            args = arguments object to read

    ***************************************************************************/

    final override protected void readArgs ( Arguments args )
    {

        super.readArgs(args);
        assert(this.dht_nodes_config.length, typeof(this).stringof
                ~ ".process -- no xml node config file");
    }
}


//Use SingleDhtTool class instead of the old DhtTool class.
deprecated alias SingleDhtTool DhtTool;


/***************************************************************************

    The class should be inherited when only one dht will be used.

***************************************************************************/

abstract class SingleDhtTool : IDhtTool
{
    /***************************************************************************

        Name of xml file which includes the dht node config

    ***************************************************************************/

    protected char[] dht_nodes_config;


    /***************************************************************************

        Dht client instance
    
    ***************************************************************************/

    protected DhtClient dht;


    /***************************************************************************

        While the initDhtClient() method performs the actual DhtNode
        initialization, however it performs that for just a single DhtNode.
        This method on the other defines how many DhtClients are required (which
        is just a single DHT in this class case).

    ***************************************************************************/

    override protected void initDhts ()
    {
        this.dht = this.initDhtClient(this.dht_nodes_config, connections);
    }


    /***************************************************************************

        Reads the tool's settings from validated command line arguments. The
        method calls the super readArgs and then parses the additional arguments
        that are not handled by the base class.

        Params:
            args = arguments object to read

    ***************************************************************************/

    final override protected void readArgs ( Arguments args )
    {

        super.readArgs(args);
        assert(this.dht_nodes_config.length, typeof(this).stringof ~ ".process -- no xml node config file");
    }

}

