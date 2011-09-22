/*******************************************************************************

    Dht node stats thread. Outputs info about the performance of the dht node to
    a trace log at intervals.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        February 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.node.servicethreads.StatsThread;


/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.node.config.MainConfig;

private import src.mod.node.servicethreads.model.IServiceThread;

private import ocean.math.SlidingAverage;

private import ocean.text.util.DigitGrouping;

private import ocean.util.log.MessageLogger;

private import ocean.util.log.StaticTrace;

private import swarm.dht.node.model.IDhtNode,
               swarm.dht.node.model.IDhtNodeInfo;

private import ocean.text.convert.Layout;

debug private import ocean.util.log.Trace;

private import tango.core.Memory;



public class StatsThread : IServiceThread
{
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
    
    private uint elapsed_since_last_log_update;
    
    
    /***************************************************************************

        Count of bytes sent & received, written to the log file

    ***************************************************************************/

    private ulong total_sent;

    private ulong total_received;


    /***************************************************************************

        Average records per second counter

    ***************************************************************************/

    private SlidingAverageTime!(ulong) records_per_sec;


    /***************************************************************************

        Strings used for free / used memory formatting.

    ***************************************************************************/

    char[] free_str;

    char[] used_str;


    /***************************************************************************
    
        Constructor.
        
        Params:
            dht = dht node to service
            update_time = time to sleep between runs of the service
    
    ***************************************************************************/
    
    public this ( IDhtNode dht, uint log_update_time )
    {
        super(dht, 1);

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
    
    protected void serviceNode ( IDhtNodeInfo node_info, uint seconds_elapsed )
    {
        auto received = node_info.bytesReceived;
        auto sent = node_info.bytesSent;

        this.records_per_sec = node_info.recordsHandled;
        auto rec_per_sec = this.records_per_sec.push;
        if ( seconds_elapsed > 1 )
        {
            for ( int i; i < seconds_elapsed - 1; i++ ) this.records_per_sec.push;
        }

        if ( MainConfig.console_stats_enabled )
        {
            size_t used, free;
            GC.usage(used, free);

            BitGrouping.format(free, this.free_str, "b");
            BitGrouping.format(used, this.used_str, "b");

            StaticTrace.format("  dht (used: {}, free: {}): handling {} connections, {} records/s",
                    this.used_str, this.free_str, node_info.numOpenConnections, rec_per_sec).flush;
        }

        if ( MainConfig.stats_log_enabled )
        {
            this.elapsed_since_last_log_update += seconds_elapsed;
            this.total_sent += sent;
            this.total_received += received;
    
            if ( this.elapsed_since_last_log_update >= this.log_update_time )
            {
                this.log.write("Node stats: {} sent ({} K/s), {} received ({} K/s), handling {} connections, {} records/s",
                        this.total_sent, cast(float)(this.total_sent / 1024) / cast(float)seconds_elapsed,
                        this.total_received, cast(float)(this.total_received / 1024) / cast(float)seconds_elapsed,
                        node_info.numOpenConnections, rec_per_sec);

                this.elapsed_since_last_log_update = 0;
                this.total_sent = 0;
                this.total_received = 0;
            }
        }

        node_info.resetCounters();
    }


    /***************************************************************************

        Method called on the channel service interface of all storage channels
        once per service run.
    
        Params:
            channel = channel service interface
            seconds_elapsed = time since this service was last performed
    
    ***************************************************************************/
    
    protected void serviceChannel ( IStorageEngineService channel, uint seconds_elapsed )
    {
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
}


/+
/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.node.servicethreads.model.IServiceThread;

private import ocean.util.TraceLog;

private import swarm.dht.node.model.IDhtNode,
               swarm.dht.node.model.IDhtNodeInfo;

private import swarm.dht.node.storage.engine.model.IStorageEngineService;

debug private import ocean.util.log.Trace;



class StatsThread : IServiceThread
{
    /***************************************************************************

        Constructor.
        
        Params:
            dht = dht node to service
            update_time = time to sleep between runs of the service
    
    ***************************************************************************/

    public this ( IDhtNode dht, uint update_time )
    {
        super(dht, update_time);
    }


    /***************************************************************************

        Method called on the node info interface once per service run. Outputs
        stats info to the trace log.

        Params:
            node_info = node information interface
            seconds_elapsed = time since this service was last performed
    
    ***************************************************************************/

    protected void serviceNode ( IDhtNodeInfo node_info, uint seconds_elapsed )
    {
        auto received = node_info.bytesReceived;
        auto sent = node_info.bytesSent;
        TraceLog.write("Node stats: {} sent ({} K/s), {} received ({} K/s), handling {} connections",
                sent, cast(float)(sent / 1024) / cast(float)seconds_elapsed,
                received, cast(float)(received / 1024) / cast(float)seconds_elapsed,
                node_info.numOpenConnections);
        node_info.resetByteCounters();
    }


    /***************************************************************************

        Method called on the channel service interface of all storage channels
        once per service run. Does nothing (required by base class).
    
        Params:
            channel = channel service interface
            seconds_elapsed = time since this service was last performed
    
    ***************************************************************************/

    protected void serviceChannel ( IStorageEngineService channel, uint seconds_elapsed )
    {
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
}

+/