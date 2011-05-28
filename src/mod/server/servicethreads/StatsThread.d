/*******************************************************************************

    Queue node stats thread. Outputs info about the performance of the queue
    node to a trace log at intervals.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.server.servicethreads.StatsThread;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.core.config.MainConfig;

private import src.mod.server.servicethreads.model.IServiceThread;

private import ocean.util.log.MessageLogger;

private import swarm.queue.node.model.IQueueNode,
               swarm.queue.node.model.IQueueNodeInfo;

private import tango.text.convert.Layout;

debug private import tango.util.log.Trace;



class StatsThread : IServiceThread
{
    /***************************************************************************

        Log file

    ***************************************************************************/

    private MessageLogger log;


    /***************************************************************************

        Channel sizes string buffer

    ***************************************************************************/

    private char[] channel_sizes;


    /***************************************************************************

        Constructor.
        
        Params:
            queue = queue node to service
            update_time = time to sleep between runs of the service
    
    ***************************************************************************/

    public this ( IQueueNode queue, uint update_time )
    {
        super(queue, update_time);

        this.log = new MessageLogger(MainConfig.stats_log, "StatsLog");
        this.log.enabled = MainConfig.stats_enabled;
        this.log.console_enabled = MainConfig.stats_console_enabled;
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
        this.formatChannelSizes(node_info, this.channel_sizes);

        auto received = node_info.bytesReceived;
        auto sent = node_info.bytesSent;
        this.log.write("Node stats: {} sent ({} K/s), {} received ({} K/s), handling {} connections{}",
                sent, cast(float)(sent / 1024) / cast(float)seconds_elapsed,
                received, cast(float)(received / 1024) / cast(float)seconds_elapsed,
                node_info.numOpenConnections,
                this.channel_sizes);

        node_info.resetByteCounters();
    }


    /***************************************************************************

        Formats the current size of each channel (in terms of % full) into the
        provided string buffer.

        Params:
            node_info = node information interface
            buf = output buffer

    ***************************************************************************/

    protected void formatChannelSizes ( IQueueNodeInfo node_info, ref char[] buf )
    {
        size_t layoutSink ( char[] s )
        {
            buf ~= s;
            return s.length;
        }

        buf.length = 0;

        foreach ( name, size, limit; node_info )
        {
            auto percent = (cast(float)size / cast(float)limit) * 100.0;
            Layout!(char).instance().convert(&layoutSink, ", {}: {}%", name, percent);
        }
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

