/*******************************************************************************

    A set of dht node service threads.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        February 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.node.servicethreads.ServiceThreads;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.node.servicethreads.model.IServiceThread;

private import ocean.util.log.Trace;



/*******************************************************************************

    Service threads set class.

    The constructor takes a delegate, which is called upon completion of all
    service threads in the set. (Each service thread executes in a loop,
    stopping on receipt of the SIGINT signal.)

*******************************************************************************/

public class ServiceThreads
{
    /***************************************************************************

        List of service threads.
    
    ***************************************************************************/

    private IServiceThread[] threads;


    /***************************************************************************

        Count of service threads which have finished.

    ***************************************************************************/

    private size_t finished_count;


    /***************************************************************************

        Delegate to call when all service threads have finished.

    ***************************************************************************/

    private void delegate ( ) finished_callback;


    /***************************************************************************

        Constructor.

        Params:
            finished_callback = delegate to call when all service threads have
                finished

    ***************************************************************************/

    public this ( void delegate ( ) finished_callback )
    in
    {
        assert(finished_callback !is null, typeof(this).stringof ~ ".ctor: finished callback is null");
    }
    body
    {
        this.finished_callback = finished_callback;
    }


    /***************************************************************************

        Adds a new service thread.

        Params:
            thread = thread to add

    ***************************************************************************/

    public void add ( IServiceThread thread )
    {
        thread.finished_callback = &this.threadFinished;
        this.threads ~= thread;
    }


    /***************************************************************************

        Starts all service threads.
    
    ***************************************************************************/

    public void start ( )
    {
        foreach ( thread; this.threads )
        {
            thread.start;
        }
    }


    /***************************************************************************

        Service thread finished callback. Each service thread will call this
        method once upon finishing. When all service threads have finished, the
        overall finished callback is invoked.

        Params:
            id = identifier string of service thread which has finished

    ***************************************************************************/

    private void threadFinished ( char[] id )
    {
        if ( ++this.finished_count == this.threads.length )
        {
            this.finished_callback();
        }
    }
}

