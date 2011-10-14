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



class ServiceThreads
{
    /***************************************************************************

        List of service threads.
    
    ***************************************************************************/

    private IServiceThread[] threads;


    /***************************************************************************

        Adds a new service thread.
        
        Params:
            thread = thread to add

    ***************************************************************************/

    public void add ( IServiceThread thread )
    {
        this.threads ~= thread;
    }


    /***************************************************************************

        Starts all service threads.
    
    ***************************************************************************/

    public void start ( )
    {
        foreach ( thread; this.threads )
        {
            thread.start();
        }
    }


    /***************************************************************************

        Stops all service threads.
    
    ***************************************************************************/

    public void stop ( )
    {
        foreach ( thread; this.threads )
        {
            thread.stop();
        }
    }


    /***************************************************************************

        Returns:
            true if one or more service threads are busy
    
    ***************************************************************************/

    public bool busy ( )
    {
        bool busy;

        foreach ( thread; this.threads )
        {
            busy = busy || thread.busy;
        }

        return busy;
    }
}

