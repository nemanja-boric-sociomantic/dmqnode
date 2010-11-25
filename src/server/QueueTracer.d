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

private import core.Terminate;

private import tango.core.Thread;

private import ocean.util.log.StaticTrace;



/*******************************************************************************

    Queue tracer class. Creates its own thread and updates the console output
    periodically with the read/write positions of the queue's channels. The
    update time is set in the config file.

    Template params:
        Q = type of queue to trace

*******************************************************************************/

class QueueTracer ( Q )
{
    /***************************************************************************

        Internal thread used for update loop
    
    ***************************************************************************/

    private Thread thread;

    
    /***************************************************************************

        Reference to the queue which is being traced
    
    ***************************************************************************/

    private Q queue;


    /***************************************************************************

        Constructor.
        
        Params:
            queue = reference to the queue to be traced
    
    ***************************************************************************/

    public this ( Q queue )
    {
        this.thread = new Thread(&this.run);
        this.queue = queue;
        this.thread.start();
    }


    /***************************************************************************

        Destructor.
    
    ***************************************************************************/

    ~this ( )
    {
        delete this.thread;
    }
    

    /***************************************************************************

        Thread run method.
    
    ***************************************************************************/

    private void run ( )
    {
        auto sleep_time = cast(float)MainConfig.channel_trace_update / 1000.0;
        
        while ( !Terminate.terminating )
        {
            foreach ( id; this.queue )
            {
                // TODO: concatenate multi-channel info with Layout
                auto channel = this.queue.channelInfo(id);
                auto read = channel.readPercent() * 100;
                auto write = channel.writePercent() * 100;
    
                if ( read <= write )
                {
                    StaticTrace.format("{} [r{}% .. w{}%]", id, read, write).flush();
                }
                else
                {
                    StaticTrace.format("{} [w{}% .. r{}%]", id, write, read).flush();
                }
            }

            Thread.sleep(sleep_time);
        }
    }
}

