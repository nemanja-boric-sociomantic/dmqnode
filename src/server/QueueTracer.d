/*******************************************************************************

    Queue Node Server Console Tracer

    copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved

    version:        November 2010: Initial release

    authors:        Gavin Norman 

*******************************************************************************/

module server.QueueTracer;



/*******************************************************************************

    Imports

*******************************************************************************/

private import core.config.MainConfig;

private import ocean.util.log.StaticTrace;

private import ocean.core.Array;

private import swarm.queue.node.model.IQueueNode;
private import swarm.queue.storage.model.StorageEngineInfo;

private import tango.core.Thread;

private import tango.text.convert.Layout;

debug private import tango.util.log.Trace;

/*******************************************************************************

    Queue tracer class. Creates its own thread and updates the console output
    periodically with the read/write positions of the queue's channels. The
    update time is set in the config file.

*******************************************************************************/

class QueueTracer : Thread
{
    /***************************************************************************

        Reference to the queue which is being traced
    
    ***************************************************************************/

    private IQueueNode queue;


    /***************************************************************************

        Output string buffer.
    
    ***************************************************************************/

    private char[] output;


    /***************************************************************************

        Run loop termination flag.
    
    ***************************************************************************/

    private bool terminated = false;
    
    /***************************************************************************

        Constructor.
        
        Params:
            queue = reference to the queue to be traced
    
    ***************************************************************************/

    public this ( IQueueNode queue )
    {
        super(&this.run);
        this.queue = queue;
    }

    /***************************************************************************

        Terminates the run loop; must be called before join() (otherwise join()
        will never return).
        
        Returns:
            this instance
    
    ***************************************************************************/
    
    typeof (this) terminate ( )
    {
        this.terminated = true;
        
        return this;
    }

    /***************************************************************************

        Thread run method.
    
    ***************************************************************************/
    
    private void run ( )
    {
        auto sleep_time = cast(float)MainConfig.channel_trace_update / 1000.0;

        while ( !this.terminated )
        {
            this.output.length = 0;
            auto num_channels = this.queue.numChannels();

            size_t i;
            foreach ( id; this.queue )
            {
                auto channel = this.queue.channelInfo(id);
                if ( channel )
                {
                    if ( MainConfig.trace_rw_positions )
                    {
                        this.appendRWTrace(id, channel);
                    }
                    else
                    {
                        this.appendSize(id, channel);
                    }
    
                    if ( i++ < num_channels - 1 )
                    {
                        this.output.append(" | ");
                    }
                }
            }
            StaticTrace.format("{}", this.output).flush();

            Thread.sleep(sleep_time);
        }
    }
    
    /***************************************************************************

        Appends read/write position info for a channel to the output string.
        
        Params:
            id = name of channel
            channel = channel info object
    
    ***************************************************************************/

    private void appendRWTrace ( char[] id, StorageEngineInfo channel )
    {
        if ( MainConfig.trace_byte_size )
        {
            auto read = channel.readPos();
            auto write = channel.writePos();
            if ( read <= write )
            {
                Layout!(char).instance().convert(&this.layoutSink, "{} [r{} .. w{}]", id, read, write);
            }
            else
            {
                Layout!(char).instance().convert(&this.layoutSink, "{} [w{} .. r{}]", id, write, read);
            }
        }
        else
        {
            auto read = channel.readPercent() * 100;
            auto write = channel.writePercent() * 100;
            if ( read <= write )
            {
                Layout!(char).instance().convert(&this.layoutSink, "{} [r{}% .. w{}%]", id, read, write);
            }
            else
            {
                Layout!(char).instance().convert(&this.layoutSink, "{} [w{}% .. r{}%]", id, write, read);
            }
        }
    }


    /***************************************************************************

        Appends size info for a channel to the output string.
        
        Params:
            id = name of channel
            channel = channel info object
    
    ***************************************************************************/

    private void appendSize ( char[] id, StorageEngineInfo channel )
    {
        if ( MainConfig.trace_byte_size )
        {
            auto size = channel.size();
            Layout!(char).instance().convert(&this.layoutSink, "{}: {}", id, size);
        }
        else
        {
            auto size = channel.sizePercent();
            Layout!(char).instance().convert(&this.layoutSink, "{}: {}%", id, size * 100.0);
        }
    }


    /***************************************************************************

        Sink delegate for Layout.convert. Appends string chunks to this.output.
        
        Params:
            s = string to process
            
        Returns:
            number of characters processed.
    
    ***************************************************************************/

    private uint layoutSink ( char[] s )
    {
        this.output.append(s);
        return s.length;
    }
}

