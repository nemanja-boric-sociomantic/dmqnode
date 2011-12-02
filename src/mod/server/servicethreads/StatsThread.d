/*******************************************************************************

    Queue node stats thread. Outputs info about the performance of the queue
    node to a trace log &/ console at intervals.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.server.servicethreads.StatsThread;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.server.config.MainConfig;

private import src.mod.server.servicethreads.model.IServiceThread;

private import ocean.math.SlidingAverage;

private import ocean.text.util.DigitGrouping;

private import ocean.text.convert.Layout;

private import ocean.util.log.MessageLogger;

private import ocean.util.log.StaticTrace;

private import swarm.queue.node.model.IQueueNodeInfo;

debug private import ocean.util.log.Trace;

private import tango.core.Memory;



public class StatsThread : IServiceThread
{
    /***************************************************************************

        Channel sizes string buffer

    ***************************************************************************/
    
    private char[] channel_sizes;


    /***************************************************************************

        Log file

    ***************************************************************************/

    private MessageLogger log;


    /***************************************************************************

        Log file update period (seconds)

    ***************************************************************************/

    private uint log_update_time;


    /***************************************************************************

        Number of seconds elapsed since the log file was last updated

    ***************************************************************************/

    uint elapsed_since_last_log_update;


    /***************************************************************************

        Count of bytes sent & received, written to the log file

    ***************************************************************************/

    ulong total_sent;

    ulong total_received;


    /***************************************************************************

        Average records per second counter

    ***************************************************************************/

    private SlidingAverageTime!(ulong) records_per_sec;


    /***************************************************************************

        String buffer for formatting.

    ***************************************************************************/

    private char[] records_buf, bytes_buf;


    /***************************************************************************

        Constructor.
        
        Params:
            queue = queue node to service
            update_time = time to sleep between runs of the service
    
    ***************************************************************************/

    public this ( IQueueNodeInfo node_info, uint log_update_time )
    {
        super(node_info, 1);

        this.records_per_sec = new SlidingAverageTime!(ulong)(5, 1_000, 1_000);

        this.log_update_time = log_update_time;

        if ( MainConfig.stats_log_enabled )
        {
            this.log = new MessageLogger(MainConfig.stats_log, "StatsLog");
            this.log.enabled = true;
        }
    }


    /***************************************************************************

        Method called on the node info interface once per service run. Outputs
        stats info to the trace log.

        Params:
            node_info = node information interface
            seconds_elapsed = time since this service was last performed

    ***************************************************************************/

    protected void serviceNode ( IQueueNodeInfo node_info, uint seconds_elapsed )
    {
        auto channels_string = this.channelSizesString(node_info);

        auto received = node_info.bytes_received;
        auto sent = node_info.bytes_sent;

        this.records_per_sec = node_info.records_handled;
        auto rec_per_sec = this.records_per_sec.push;
        if ( seconds_elapsed > 1 )
        {
            for ( int i; i < seconds_elapsed - 1; i++ ) this.records_per_sec.push;
        }

        if ( MainConfig.console_stats_enabled )
        {
            DigitGrouping.format(node_info.num_records, this.records_buf);
            BitGrouping.format(node_info.num_bytes, this.bytes_buf, "b");

            StaticTrace.format("  {} queue: handling {} connections, {} records/s, {} records ({})",
                    node_info.storage_type, node_info.num_open_connections, rec_per_sec,
                    this.records_buf, this.bytes_buf).flush;
        }

        if ( MainConfig.stats_log_enabled )
        {
            this.elapsed_since_last_log_update += seconds_elapsed;
            this.total_sent += sent;
            this.total_received += received;

            if ( this.elapsed_since_last_log_update >= this.log_update_time )
            {
                this.log.write("Node stats: {} sent ({} K/s), {} received ({} K/s), handling {} connections, {} records/s{}",
                        this.total_sent, cast(float)(this.total_sent / 1024) / cast(float)seconds_elapsed,
                        this.total_received, cast(float)(this.total_received / 1024) / cast(float)seconds_elapsed,
                        node_info.num_open_connections, rec_per_sec,
                        channels_string);

                this.elapsed_since_last_log_update = 0;
                this.total_sent = 0;
                this.total_received = 0;
            }
        }

        node_info.resetCounters();
    }


    /***************************************************************************

        Gets the identifying string for this class (used for message printing).
    
        Returns:
            class id
    
    ***************************************************************************/
    
    protected char[] id ( )
    {
        return typeof(this).stringof;
    }


    /***************************************************************************
    
        Formats the current size of each channel (in terms of % full) into the
        provided string buffer.
    
        Params:
            node_info = node information interface
    
    ***************************************************************************/

    private char[] channelSizesString ( IQueueNodeInfo node_info )
    {
        this.channel_sizes.length = 0;

        foreach ( channel_info; node_info )
        {
            auto percent = (cast(float)channel_info.num_bytes /
                            cast(float)node_info.channelSizeLimit) * 100.0;
            Layout!(char).print(this.channel_sizes, ", {}: {}%", channel_info.id, percent);
        }
    
        return this.channel_sizes;
    }
}

