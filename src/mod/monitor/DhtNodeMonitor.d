/*******************************************************************************

    DHT node monitor daemon

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        Jun 2010: Initial release

    authors:        Gavin Norman

--

	Displays an updating count of the number of records in each channel of the
	DHT node specified in the config file.

*******************************************************************************/

module src.mod.monitor.DhtNodeMonitor;



/*******************************************************************************

	Imports

*******************************************************************************/

private import core.config.MainConfig;

private import tango.core.Thread;

private import tango.core.Array;

private import swarm.dht.DhtClient;

private import tango.util.log.Trace;



/*******************************************************************************

	DhtNodeMonitor - starts the monitor daemon

*******************************************************************************/

struct DhtNodeMonitor
{
	static NodeMonDaemon daemon;
	
	static bool run ( )
    {
		daemon = new NodeMonDaemon();
		daemon.run();
		return true;
    }
}



/*******************************************************************************

	DHT node monitor daemon

*******************************************************************************/

class NodeMonDaemon : DhtClient
{
	/***************************************************************************

		Number of seconds between display updates

	***************************************************************************/

	static public const WAIT_TIME = 60;


	/***************************************************************************

		DHT node address - read from config file by constructor

	***************************************************************************/

	protected char[] node_address;


	/***************************************************************************

		DHT node port - read from config file by constructor

	***************************************************************************/

	protected ushort node_port;
    

	/***************************************************************************

		List of channels in DHT node - created by constructor

	***************************************************************************/

	protected char[][] channels;


	/***************************************************************************

		Constructor
	
	***************************************************************************/

	public this ( )
    {
    	new Thread(&this.run);

    	this.node_address  = Config.getChar("Server", "address");
        this.node_port     = Config.getInt("Server", "port");

        Trace.formatln("Monitoring DHT node at {}:{}", this.node_address, this.node_port);

        this.getAllChannels(this.channels);
    }


	/***************************************************************************

		Daemon main loop. Updates the display, then sleeps a while - on infinite
		loop.
	
	***************************************************************************/

	public void run ( )
    {
    	while ( true )
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
		Trace.formatln("-----------------------------------------------------");
    	foreach ( channel; this.channels )
    	{
    		ulong records;
    		ulong bytes;
    		
    		this.getChannelSize(this.node_address, this.node_port, channel, records, bytes);

    		Trace.formatln("{,20}: {,12} = {,6}M", channel, records, cast(double)records / 1000_000.0);
    	}
    }
    

	/***************************************************************************

		Creates the list of channels in the DHT node.
	
	***************************************************************************/

	protected void getAllChannels ( out char[][] channel_names )
    {
    	this.addNode(this.node_address, this.node_port, 0x0, 0xF);
    	
    	char[][] node_channels;
		this.getChannels(this.node_address, this.node_port, node_channels);
		
		foreach ( channel; node_channels )
		{
			if ( !channel_names.contains(channel) )
			{
    			channel_names ~= channel;
			}
		}
    }
}

