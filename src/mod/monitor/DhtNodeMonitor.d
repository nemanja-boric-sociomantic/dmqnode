/*******************************************************************************

    DHT node monitor daemon

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        Jun 2010: Initial release

    authors:        Gavin Norman

    --

	Displays an updating count of the number of records in each channel of the
	DHT node specified in the config file.

 ******************************************************************************/

module src.mod.monitor.DhtNodeMonitor;



/*******************************************************************************

	Imports

 ******************************************************************************/

private import  core.config.MainConfig;

private import  swarm.dht.async.AsyncDhtClient;

private import  swarm.dht.DhtHash, swarm.dht.DhtConst;

private import  tango.core.Thread;

private import  tango.core.Array;

private import  tango.time.Clock;

private import  tango.util.Arguments;

private import  tango.util.log.Trace;

private import  Integer = tango.text.convert.Integer;



/*******************************************************************************

	DhtNodeMonitor - starts the monitor daemon

 ******************************************************************************/

struct DhtNodeMonitor
{
	static NodeMonDaemon daemon;
	
	public static bool run ( Arguments args )
    {
		daemon = new NodeMonDaemon();
        
        if (args.contains("d"))
        {
    		daemon.run();
        }
        else
        {
            daemon.update();
        }
        
		return true;
    }
}



/*******************************************************************************

	DHT node monitor daemon

 ******************************************************************************/

class NodeMonDaemon : AsyncDhtClient
{
    /***************************************************************************

        DHT configuration file 
    
     **************************************************************************/
    
    const char[][] DhtNodeCfg     = ["etc", "dhtnodes.xml"];
    
	/***************************************************************************

		Number of seconds between display updates

	 **************************************************************************/

	static public const WAIT_TIME = 60;


	/***************************************************************************

		DHT node address - read from config file by constructor

	 **************************************************************************/

	protected char[] node_address;


	/***************************************************************************

		DHT node port - read from config file by constructor

	 **************************************************************************/

	protected ushort node_port;
    

	/***************************************************************************

		List of channels in DHT node - created by constructor

	 **************************************************************************/

	protected char[][] channels;
    
    /***************************************************************************

        Minimum value for responsible range
    
     **************************************************************************/
        
    protected char[] range_min;
    
    /***************************************************************************

        Maximum value for responsible range
    
     **************************************************************************/
    
    protected char[] range_max;
    
    /***************************************************************************

        Number of bytes for a channel
    
     **************************************************************************/
    
    protected ulong c_bytes[char[]][char[]];
    
    /***************************************************************************
    
        Number of records for a channel
    
     **************************************************************************/
        
    protected ulong c_records[char[]][char[]];
    
    protected uint node_index = 0;
    
    /***************************************************************************

        Total number of bytes for all channels
    
     **************************************************************************/
    
    protected ulong t_bytes[char[]];

    /***************************************************************************

        Total number of records for all channels
    
     **************************************************************************/
        
    protected ulong t_records[char[]];
    
    protected char[] node_id;
    
    /***************************************************************************

        Buffer for thousand separator method
        
        TODO
    
     **************************************************************************/
    
	protected char[] buf;

	/***************************************************************************

		Constructor
        
        Params:
            exepath = path to running executable as given by command line
                      argument 0
	
	 **************************************************************************/

	public this ()
    {
        hash_t range_min, range_max;
        DhtHash.HexDigest hash;
        
    	new Thread(&this.run);
        
        this.node_address  = Config.getChar("Server", "address");
        this.node_port     = Config.getInt("Server", "port");
        
        foreach (node; MainConfig.getDhtNodeItems())
        {
            this.addNode(node.Address, node.Port);
        }
    }

	/***************************************************************************

		Daemon main loop. Updates the display, then sleeps a while - on infinite
		loop.
	
        Returns:
            void
            
	***************************************************************************/

	public void run ()
    {
    	while (true)
    	{
    		this.update();
    		Thread.sleep(WAIT_TIME);
    	}
    }

	/***************************************************************************

		Updates the display. Queries the DHT node for the number of records in
		all channels.
        
        Returns:
            void
	
	***************************************************************************/

	protected void update ()
    {
        this.getChannels(&this.addChannels);
        this.eventLoop();
		
    	foreach (channel; this.channels)
    	{	
    		this.getChannelSize(channel, &this.addChannelSize);  
            this.eventLoop();
    	}

        this.print();
    }
    
    
    private void print ()
    {  
        uint i = 0;
    
        this.printLine();
        
        // Trace.formatln("Responsible Key Range: \t{} - {}", this.range_min, this.range_max);
        
        Trace.formatln(" Time: {}", Clock.now());
        
        this.printLine();
        
        Trace.format("{,21} |", "");
        
        foreach (node; MainConfig.getDhtNodeItems())
        {
            this.node_id = node.Address ~ ":" ~ Integer.toString(node.Port);
            Trace.format("{,33} |", this.node_id);
        }
        Trace.formatln("");        
        
        this.printLine();
        
        Trace.format("{,21} |", "Channel");
        
        foreach (node; MainConfig.getDhtNodeItems())
        {
            Trace.format("{,11} | {,19} |", "Items", "Size");
        }
        
        Trace.formatln("");        
        
        this.printLine();
            
        foreach (channel; this.channels)
        {
            Trace.format("{,21} |", channel);
            
            foreach (node; MainConfig.getDhtNodeItems())
            {
                this.node_id = node.Address ~ ":" ~ Integer.toString(node.Port);
                
                Trace.format("{,11} | {,13} bytes |",
                        typeof(this).formatCommaNumber(this.c_records[this.node_id][channel], this.buf),
                        typeof(this).formatCommaNumber(this.c_bytes[this.node_id][channel], this.buf));         
                
                this.t_records[this.node_id]  += this.c_records[this.node_id][channel];
                this.t_bytes[this.node_id]    += this.c_bytes[this.node_id][channel];                
            }
            Trace.formatln("");
        }
        
        this.printLine();
        
        Trace.format("{,21} |", "Total"); 
        
        foreach (node; MainConfig.getDhtNodeItems())
        {
            this.node_id = node.Address ~ ":" ~ Integer.toString(node.Port);
            
            Trace.format("{,11} | {,13} bytes |",
                typeof(this).formatCommaNumber(this.t_records[this.node_id], this.buf), 
                typeof(this).formatCommaNumber(this.t_bytes[this.node_id], this.buf));
        }
        Trace.formatln("");
        this.printLine();    
    }
    
    
    
    private void printLine ()
    {
        Trace.format("-----------------------");   
        
        foreach (node; MainConfig.getDhtNodeItems())
        {
            Trace.format("-----------------------------------");   
        }
        Trace.formatln("");
    }
    
	/***************************************************************************

		Creates the list of channels in the DHT node. The method is
        called by AsyncDhtClient.getChannel.
        
        Param:
            channel = name of the channel
            
        Returns:
            void
       
	 **************************************************************************/

	protected void addChannels ( char[] channel )
    {	
        if (!this.channels.contains(channel))
		{            
    		this.channels ~= channel.dup;		
		}
    }

    /***************************************************************************

        Retrieves information about the node. This method is called by
        AsyncDhtClient.getChannelSize
        
        Params:
            address = node IP address
            port = node port
            channel = node channel name 
            records = number of records
            bytes = number of bytes
            
        Returns:
            void            

     **************************************************************************/
    
    private void addChannelSize ( char[] address, ushort port, char[] channel, 
            ulong records, ulong bytes )
    {
        this.node_id = address ~ ":" ~ Integer.toString(port);
        
        this.c_bytes[node_id][channel] = bytes;
        this.c_records[node_id][channel] = records;
    }
    
    
	/***************************************************************************

		Formats a number to a string, with comma separation every 3 digits
	
	 **************************************************************************/

	protected static char[] formatCommaNumber ( T ) ( T num, out char[] str )
    {
    	auto string = Integer.toString(num);

    	bool comma;
		size_t left = 0;
		size_t right = left + 3;
		size_t first_comma;

		if ( string.length > 3 )
		{
			comma = true;
			first_comma = string.length % 3;

			if ( first_comma > 0 )
			{
				right = first_comma;
			}
		}

		do
		{
			if ( right >= string.length )
			{
				right = string.length;
				comma = false;
			}
			str ~= string[left..right];
			if ( comma )
			{
				str ~= ",";
			}
			
			left = right;
			right = left + 3;
		} while( left < string.length );

		return str;
    }
}

