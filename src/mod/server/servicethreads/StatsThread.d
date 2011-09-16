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

private import ocean.text.util.DigitGrouping;

private import ocean.text.convert.Layout;

private import ocean.util.log.MessageLogger;

private import ocean.util.log.StaticTrace;

private import swarm.queue.node.model.IQueueNode,
               swarm.queue.node.model.IQueueNodeInfo;

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

        Strings used for free / used memory formatting.
    
    ***************************************************************************/
    
    char[] free_str;
    
    char[] used_str;


    /***************************************************************************

        Constructor.
        
        Params:
            queue = queue node to service
            update_time = time to sleep between runs of the service
    
    ***************************************************************************/

    public this ( IQueueNode queue, uint log_update_time )
    {
        super(queue, 1);

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

        auto received = node_info.bytesReceived;
        auto sent = node_info.bytesSent;

        if ( MainConfig.console_stats_enabled )
        {
            size_t used, free;
            GC.usage(used, free);

            BitGrouping.format(free, this.free_str, "b");
            BitGrouping.format(used, this.used_str, "b");

            StaticTrace.format("  queue (used: {}, free: {}): {} sent ({} K/s), {} received ({} K/s), handling {} connections{}",
                    this.used_str, this.free_str, sent, cast(float)(sent / 1024) / cast(float)seconds_elapsed,
                    received, cast(float)(received / 1024) / cast(float)seconds_elapsed,
                    node_info.numOpenConnections,
                    channels_string).flush;
        }

        if ( MainConfig.stats_log_enabled )
        {
            this.elapsed_since_last_log_update += seconds_elapsed;
            this.total_sent += sent;
            this.total_received += received;

            if ( this.elapsed_since_last_log_update >= this.log_update_time )
            {
                this.log.write("Node stats: {} sent ({} K/s), {} received ({} K/s), handling {} connections{}",
                        this.total_sent, cast(float)(this.total_sent / 1024) / cast(float)seconds_elapsed,
                        this.total_received, cast(float)(this.total_received / 1024) / cast(float)seconds_elapsed,
                        node_info.numOpenConnections,
                        channels_string);

                this.elapsed_since_last_log_update = 0;
                this.total_sent = 0;
                this.total_received = 0;
            }
        }

        node_info.resetByteCounters();
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
    
        foreach ( name, size, limit; node_info )
        {
            auto percent = (cast(float)size / cast(float)limit) * 100.0;
            Layout!(char).print(this.channel_sizes, ", {}: {}%", name, percent);
        }
    
        return this.channel_sizes;
    }
}

