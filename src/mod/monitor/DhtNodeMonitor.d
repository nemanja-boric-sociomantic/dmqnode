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

private import  swarm.dht.DhtHash;

private import  tango.core.Thread;

private import  tango.core.Array;

private import  tango.time.Clock;

private import  tango.util.Arguments;

private import  tango.util.log.Trace;



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

        Total number of records for all channels
    
     **************************************************************************/
        
    protected ulong t_records;
    
    /***************************************************************************

        Minimum value for responsible range
    
     **************************************************************************/
        
    protected char[] range_min;
    
    /***************************************************************************

        Maximum value for responsible range
    
     **************************************************************************/
    
    protected char[] range_max;
    
    /***************************************************************************

        Total number of bytes for all channels
    
     **************************************************************************/
    protected ulong t_bytes;

	// TODO
	protected char[] buf;



	/***************************************************************************

		Constructor
	
	 **************************************************************************/

	public this ( )
    {
        hash_t range_min, range_max;
        DhtHash.HexDigest hash;
        
    	new Thread(&this.run);
        
    	this.node_address  = Config.getChar("Server", "address");
        this.node_port     = Config.getInt("Server", "port");

        this.addNode(this.node_address, this.node_port);
        
//        this.getResponsibleRange(this.node_address, this.node_port, range_min, range_max);
//        
//        this.range_min = DhtHash.toHashStr(range_min, hash).dup;
//        this.range_max = DhtHash.toHashStr(range_max, hash).dup;
    }


	/***************************************************************************

		Daemon main loop. Updates the display, then sleeps a while - on infinite
		loop.
	
	***************************************************************************/

	public void run ( )
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
	
	***************************************************************************/

	protected void update ( )
    {
        this.getAllChannels();

		this.t_records = 0;
		this.t_bytes = 0;
		
        Trace.formatln("-----------------------------------------------------------------");
        Trace.formatln("Node: \t\t\t{}:{}", this.node_address, this.node_port);
        Trace.formatln("Responsible Key Range: \t{} - {}", this.range_min, this.range_max);
        Trace.formatln("Time: \t\t\t{}", Clock.now());
        Trace.formatln("-----------------------------------------------------------------");
        Trace.formatln("\n{,21} {,15} {,26}", "Channel", "Items", "Size");
        Trace.formatln("-----------------------------------------------------------------");
        
    	foreach (channel; this.channels)
    	{
    		ulong[] info;
    		
    		this.getChannelSize(channel, info);  
            this.eventLoop();

            this.t_records  += info[0];
            this.t_bytes    += info[1];
            
            Trace.formatln("{,20}: {,15} {,20} bytes", channel, 
                    typeof(this).formatCommaNumber(info[0], this.buf),
                    typeof(this).formatCommaNumber(info[1], this.buf));            
    	}
        
        Trace.formatln("-----------------------------------------------------------------");
        Trace.formatln("{,20}  {,15} {,20} bytes", "", 
                typeof(this).formatCommaNumber(this.t_records, this.buf), 
                typeof(this).formatCommaNumber(this.t_bytes, this.buf));
        Trace.formatln("-----------------------------------------------------------------\n");
    }
    

	/***************************************************************************

		Creates the list of channels in the DHT node.
       
	 **************************************************************************/

	protected void getAllChannels ()
    {
    	char[][] node_channels;
		this.getChannels(node_channels);
        this.eventLoop();
		
		foreach (channel; node_channels)
		{
			if (!this.channels.contains(channel))
			{
    			this.channels ~= channel;
			}
		}
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

