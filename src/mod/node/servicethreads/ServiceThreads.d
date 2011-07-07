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
}

