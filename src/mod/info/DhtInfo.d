/*******************************************************************************

    DHT node info

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman

    Display information about a dht - the names of the channels, and optionally
    the number of records & bytes per channel.

    Command line parameters:
        -S = dhtnodes.xml file for dht to query
        -n = display the count of records & bytes per channel

    Inherited from super class:
        -h = display help

*******************************************************************************/

module src.mod.info.DhtInfo;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.model.DhtTool;

private import swarm.dht.DhtClient,
           swarm.dht.DhtHash,
           swarm.dht.DhtConst;

private import ocean.core.Array;

private import ocean.text.Arguments;

private import tango.core.Array;

private import tango.io.Stdout;

debug private import tango.util.log.Trace;



/*******************************************************************************

    Dht info tool

*******************************************************************************/

class DhtInfo : DhtTool
{
    /***************************************************************************
    
        Singleton parseArgs() and run() methods.
    
    ***************************************************************************/
    
    mixin SingletonMethods;


    /***************************************************************************
    
        Toggles display of records & bytes per channel
    
    ***************************************************************************/
    
    private bool show_record_count;


    /***************************************************************************

        Main process method. Runs the tool based on the passed command line
        arguments.
    
        Params:
            dht = dht client to use
    
    ***************************************************************************/
    
    protected void process_ ( DhtClient dht )
    {
        char[][] channels;
        dht.getChannels(
                ( uint id, char[] channel )
                {
                    if ( channel.length && !channels.contains(channel) )
                    {
                        channels.appendCopy(channel);
                    }
                }
            ).eventLoop();
    
        if ( this.show_record_count )
        {
            foreach ( channel; channels )
            {
                ulong channel_records, channel_bytes;
    
                Stdout.formatln("Channel '{}':", channel);
    
                this.displayChannelSize(dht, channel, channel_records, channel_bytes);
    
                Stdout.formatln("Total for channel = {} records, {} bytes\n", channel_records, channel_bytes);
            }
        }
        else
        {
            Stdout.formatln("Channels:");
            foreach ( channel; channels )
            {
                Stdout.formatln("  {}", channel);
            }
        }
    }


    /***************************************************************************
    
        Adds command line arguments specific to this tool.
        
        Params:
            args = command line arguments object to add to
    
    ***************************************************************************/

    override protected void addArgs_ ( Arguments args )
    {
        args("source").params(1).required().aliased('S').help("path of dhtnodes.xml file defining nodes to query");
        args("count").aliased('n').help("display the number of records and bytes per channel per node");
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
        if ( !args.exists("source") )
        {
            Stderr.formatln("No xml source file specified (use -S)");
            return false;
        }

        return true;
    }
    
    
    /***************************************************************************
    
        Initialises this instance from the specified command line args.
    
        Params:
            args = command line arguments object to read settings from
    
    ***************************************************************************/
    
    override protected void readArgs ( Arguments args )
    {
        super.dht_nodes_config = args.getString("source");

        this.show_record_count = args.getBool("count");
    }


    /***************************************************************************
    
        Outputs the size and record count of the specified channel to stdout.
    
        Params:
            dht = dht client to perform query with
            channel = channel to query
    
    ***************************************************************************/

    private void displayChannelSize ( DhtClient dht, char[] channel, ref ulong channel_records, ref ulong channel_bytes )
    {
        dht.getChannelSize(channel,
                ( hash_t id, char[] address, ushort port, char[] channel, ulong records, ulong bytes )
                {
                    channel_records += records;
                    channel_bytes += bytes;

                    Stdout.formatln("  node {}:{} - {} records, {} bytes", address, port, records, bytes);
                }).eventLoop();
    
        Stdout.flush();
    }
}

