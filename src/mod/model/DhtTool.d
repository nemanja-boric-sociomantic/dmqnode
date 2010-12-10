/*******************************************************************************

    DHT node tool abstract class
    
    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        December 2010: Initial release
    
    authors:        Gavin Norman

    Base class for DHT tools which connect to a node cluster specified in an xml
    file, and provide commands over single keys, key sub-ranges and the complete
    hash range, and over a specified channel or all channels.
    
    Provides the following command line parameters:
        -h = display help
        -S = dhtnodes.xml source file
        -k = process just a single record with the specified key (hash)
        -s = start of range to process (hash value - defaults to 0x00000000)
        -e = end of range to process (hash value - defaults to 0xFFFFFFFF)
        -C = process complete hash range
        -c = channel name to process
        -A = process all channels

*******************************************************************************/

module src.mod.model.DhtTool;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.core.Array;

private import ocean.text.Arguments;

private import swarm.dht.DhtClient,
               swarm.dht.DhtHash,
               swarm.dht.DhtConst;

private import swarm.dht.client.connection.ErrorInfo;

private import swarm.dht.client.DhtNodesConfig;

private import tango.io.Stdout;



/*******************************************************************************

    Dht tool abstract class

*******************************************************************************/

abstract class DhtTool
{
    /***************************************************************************

        Name of xml file which includes the dht node config

    ***************************************************************************/

    protected char[] xml;


    /***************************************************************************

        Flag indicating whether a dht error has occurred
    
    ***************************************************************************/

    protected bool dht_error;


    /***************************************************************************

        Query key range struct
    
    ***************************************************************************/

    struct Range
    {
        enum RangeType
        {
            SingleKey,
            KeyRange
        }

        RangeType type;
        
        hash_t key1, key2;
    }

    protected Range range;


    /***************************************************************************

        Query channels struct
    
    ***************************************************************************/

    struct Channels
    {
        bool all_channels;
        
        char[] channel;
    }

    protected Channels channels;


    /***************************************************************************

        Parses and validates command line arguments using the passed Arguments
        object. The list of valid arguments for the base class (see module
        header) is set in the addArgs() method. Derived classes can override the
        addArgs_() method to specify additional arguments.
    
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
    
        if ( !args.parse(arguments) )
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

        auto dht = this.initDhtClient(this.xml);

        if ( this.channels.all_channels )
        {
            with ( this.range.RangeType ) switch ( this.range.type )
            {
                case KeyRange:
                    this.processAllChannels(dht, this.range.key1, this.range.key2);
                    break;
            }
        }
        else
        {
            with ( this.range.RangeType ) switch ( this.range.type )
            {
                case SingleKey:
                    this.processRecord(dht, this.channels.channel, this.range.key1);
                    break;
    
                case KeyRange:
                    this.processChannel(dht, this.channels.channel, this.range.key1, this.range.key2);
                    break;
            }
        }
    
        this.finished(dht);
    }


    /***************************************************************************

        Runs the tool over the specified hash range on a single channel.

        Params:
            dht = dht client
            channel = name of channel
            start = start of hash range
            end = end of hash range
    
    ***************************************************************************/

    abstract protected void processChannel ( DhtClient dht, char[] channel, hash_t start, hash_t end );


    /***************************************************************************

        Runs the tool over the specified record in a single channel.

        Params:
            dht = dht client
            channel = name of channel
            key = record hash
    
    ***************************************************************************/

    abstract protected void processRecord ( DhtClient dht, char[] channel, hash_t key);


    /***************************************************************************

        Called at the end of processing (in the process() method, above). The
        base class implementation does nothing, but derived classes may wish to
        add behaviour at this point.

        Params:
            dht = dht client

    ***************************************************************************/

    protected void finished ( DhtClient dht )
    {
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

        Validates command line arguments in the passed Arguments object. The
        base class' arguments (see module header) are validated by the
        validArgs() method. Derived classes should override this method to
        validate any additional arguments.
    
        Params:
            args = arguments object used to parse command line arguments
    
        Returns:
            true if the command line args are valid
    
    ***************************************************************************/

    protected bool validArgs_ ( Arguments args )
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
    
    protected void readArgs_ ( Arguments args )
    in
    {
        assert(this.validArgs(args), typeof(this).stringof ~ "readArgs_ - invalid arguments");
    }
    body
    {
    }


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
        args("source").params(1).required().aliased('S').help("path of dhtnodes.xml file defining nodes to dump");
        args("key").params(1).aliased('k').help("fetch just a single record with the specified key (hash)");
        args("start").params(1).aliased('s').help("start of range to query (hash value - defaults to 0x00000000)");
        args("end").params(1).aliased('e').help("end of range to query (hash value - defaults to 0xFFFFFFFF)");
        args("complete_range").aliased('C').help("fetch records in the complete hash range");
        args("channel").conflicts("all_channels").params(1).aliased('c').help("channel name to query");
        args("all_channels").conflicts("channel").aliased('A').help("query all channels");

        this.addArgs_(args);
    }


    /***************************************************************************

        Validates command line arguments in the passed Arguments object. This
        method validates only the base class' arguments (see module header),
        then calls the validArgs_() method to validate any additional command
        line arguments required by the derived class.
    
        Params:
            args = arguments object used to parse command line arguments
    
        Returns:
            true if the command line args are valid
    
    ***************************************************************************/
    
    private bool validArgs ( Arguments args )
    {
        if ( !args.exists("source") )
        {
            Stderr.formatln("No xml source file specified (use -S)");
            return false;
        }
        
        bool all_channels = args.exists("all_channels");
        bool one_channel = args.exists("channel");
    
        if ( !oneTrue(all_channels, one_channel) )
        {
            Stderr.formatln("Please specify exactly one of the following options: single channel (-c) or all channels (-A)");
            return false;
        }
    
        bool complete_range = args.exists("complete_range");
        bool key_range = args.exists("start") || args.exists("end");
        bool single_key = args.exists("key");
    
        if ( !oneTrue(complete_range, key_range, single_key) )
        {
            Stderr.formatln("Please specify exactly one of the following options: complete range (-C), key range (-s .. -e) or single key (-k)");
            return false;
        }
        
        if ( single_key && all_channels )
        {
            Stderr.formatln("Cannot process a single key (-k) over all channels (-A)");
            return false;
        }
    
        return this.validArgs_(args);
    }


    /***************************************************************************

        Reads the tool's settings from validated command line arguments. This
        method reads only the base class' arguments (see module header), then
        calls the readArgs_() method to read any additional command line
        arguments required by the derived class.

        Params:
            args = arguments object to read
    
    ***************************************************************************/

    private void readArgs ( Arguments args )
    in
    {
        assert(this.validArgs(args), typeof(this).stringof ~ "readArgs - invalid arguments");
    }
    body
    {
        this.xml = args.getString("source");
        
        if ( args.exists("all_channels") )
        {
            this.channels.all_channels = true;
            this.channels.channel.length = 0;
        }
        else if ( args.exists("channel") )
        {
            this.channels.all_channels = false;
            this.channels.channel = args.getString("channel");
        }

        if ( args.exists("complete_range") )
        {
            this.range.type = this.range.type.KeyRange;
            this.range.key1 = 0x00000000;
            this.range.key2 = 0xffffffff;
        }
        else if ( args.exists("start") || args.exists("end") )
        {
            this.range.type = this.range.type.KeyRange;
            this.range.key1 = args.exists("start") ? args.getInt!(hash_t)("start") : 0x00000000;
            this.range.key2 = args.exists("end") ? args.getInt!(hash_t)("end") : 0xffffffff;
        }
        else if ( args.exists("key") )
        {
            this.range.type = this.range.type.SingleKey;
            this.range.key1 = args.getInt!(hash_t)("key");
            this.range.key2 = this.range.key1;
        }

        this.readArgs_(args);
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

    private DhtClient initDhtClient ( char[] xml )
    {
        auto dht = new DhtClient();
        
        dht.error_callback(&this.dhtError);
        
        DhtNodesConfig.addNodesToClient(dht, xml);
        dht.nodeHandshake();
        assert(!this.dht_error);
    
        return dht;
    }


    /***************************************************************************

        Dht error callback. Outputs the error message to the console and sets an
        internal flag to indicate that an error has occurred.

        Params:
            e = error info

    ***************************************************************************/

    private void dhtError ( ErrorInfo e )
    {
        Stderr.format("DHT client error: {}\n", e.message);
        this.dht_error = true;
    }


    /***************************************************************************

        Runs the tool over the specified hash range on all channels in the
        dht node cluster. The channels are processed in series.

        Params:
            dht = dht client
            start = start of hash range
            end = end of hash range

    ***************************************************************************/
    
    private void processAllChannels ( DhtClient dht, hash_t start, hash_t end )
    {
        char[][] channels;
        dht.getChannels(
                ( uint id, char[] channel )
                {
                    if ( channel.length )
                    {
                        channels.appendCopy(channel);
                    }
                }
            ).eventLoop();
    
        foreach ( channel; channels )
        {
            this.processChannel(dht, channel, start, end);
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


    /***************************************************************************

        Template used to mixin static singleton methods to derived classes.
    
    ***************************************************************************/

    template SingletonMethods ( )
    {
        /***********************************************************************
        
            Singleton instance of this class, used in static methods.
        
        ***********************************************************************/
        
        private static typeof(this) singleton;
        
        static private typeof(this) instance ( )
        {
            if ( !singleton )
            {
                singleton = new typeof(this);
            }
        
            return singleton;
        }
        
        
        /***********************************************************************
        
            Parses and validates command line arguments.
            
            Params:
                args = arguments object
                arguments = command line args (excluding the file name)
        
            Returns:
                true if the arguments are valid
        
        ***********************************************************************/
        
        static public bool parseArgs ( Arguments args, char[][] arguments )
        {
            return instance().validateArgs(args, arguments);
        }


        /***********************************************************************
        
            Main run method, called by OceanException.run.
            
            Params:
                args = processed arguments
        
            Returns:
                always true
        
        ***********************************************************************/
        
        static public bool run ( Arguments args )
        {
            instance().process(args);
            return true;
        }
    }
}

