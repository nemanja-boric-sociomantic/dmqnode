/*******************************************************************************

    DHT node tool abstract class

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        December 2010: Initial release

    authors:        Gavin Norman

    Base class for DHT tools which connect to a node cluster specified in an xml
    file.

    Provides the following command line parameters:
        -h = display help

*******************************************************************************/

module src.mod.model.DhtTool;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.io.select.EpollSelectDispatcher;

private import ocean.text.Arguments : Arguments;

private import swarm.dht.DhtClient,
               swarm.dht.DhtHash,
               swarm.dht.DhtConst;

private import tango.io.Stdout;



/*******************************************************************************

    Dht tool abstract class

*******************************************************************************/

abstract class DhtTool
{
    /***************************************************************************

        Name of xml file which includes the dht node config

    ***************************************************************************/

    protected char[] dht_nodes_config;


    /***************************************************************************

        Flag indicating whether a dht error has occurred
    
    ***************************************************************************/

    protected bool dht_error;


    /***************************************************************************

        Epoll selector instance
    
    ***************************************************************************/

    protected EpollSelectDispatcher epoll;


    /***************************************************************************

        Dht client instance
    
    ***************************************************************************/

    protected DhtClient dht;


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

    final public bool parseArgs ( char[] exe_name, Arguments args, char[][] arguments )
    {
        this.addArgs(args);

        if ( !args.parse(arguments) )
        {
            if ( !args.exists("help") )
            {
                Stderr.formatln("Invalid arguments:");
                args.displayErrors();
            }

            args.displayHelp(exe_name);

            return false;
        }

        if ( this.validArgs(args) )
        {
            return true;
        }
        else
        {
            args.displayHelp(exe_name);

            return false;
        }
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
        assert(this.validArgs(args), typeof(this).stringof ~ ".process -- invalid arguments");
    }
    body
    {
        this.readArgs(args);

        assert(this.dht_nodes_config.length, typeof(this).stringof ~ ".process -- no xml node config file");

        this.epoll = new EpollSelectDispatcher;

        this.dht = this.initDhtClient(this.dht_nodes_config);

        this.init();

        this.process_();

        this.finished();
    }


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
    
        Params:
            dht = dht client
    
    ***************************************************************************/
    
    protected void init (  )
    {
    }


    /***************************************************************************

        Called at the end of processing (in the process() method, above). The
        base class implementation does nothing, but derived classes may wish to
        add behaviour at this point.

        Params:
            dht = dht client

    ***************************************************************************/

    protected void finished (  )
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
        return true;
    }


    /***************************************************************************

        Reads the tool's settings from validated command line arguments. The
        base class' arguments (see module header) are read by the readArgs()
        method. Derived classes should override this method to read any
        additional arguments.

        Params:
            args = arguments object to read
    
    ***************************************************************************/
    
    final protected void readArgs ( Arguments args )
    in
    {
        assert(this.validArgs(args), typeof(this).stringof ~ ".readArgs_: invalid arguments");
    }
    body
    {
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

    private void addArgs ( Arguments args )
    {
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

        Initialises a dht client, connecting to nodes in the cluster specified
        in the xml config file.

        Params:
            xml = name of xml file defining nodes in dht cluster

        Returns:
            initialised dht client

        Throws:
            asserts that no errors occurred during initialisation

    ***************************************************************************/

    protected DhtClient initDhtClient ( char[] xml, uint request_queue_size = 1000 )
    {
        Stderr.formatln("Initialising dht client connections from {}", xml);

        const num_conn = 10;

        auto dht = new DhtClient(this.epoll, num_conn, request_queue_size);

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
            assert(!this.dht_error, typeof(this).stringof ~ ".initDhtClient - error during dht client initialisation of " ~ xml);
        }

        Stderr.formatln("Dht client connections initialised");

        return dht;
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

        Dht error callback. Outputs the error message to the console and sets an
        internal flag to indicate that an error has occurred.

        Params:
            e = error info

    ***************************************************************************/

    protected void notifier ( DhtClient.RequestNotification info )
    {
        if ( info.type == info.type.Finished && !info.succeeded )
        {
            Stderr.format("DHT client error: {}\n", info.message);
            this.dht_error = true;
        }
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

