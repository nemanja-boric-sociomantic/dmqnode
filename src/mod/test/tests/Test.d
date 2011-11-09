/*******************************************************************************

    Abstract test class

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.tests.Test;

/*******************************************************************************

    Swarm Imports

*******************************************************************************/
           
private import swarm.dht.DhtClient;

/*******************************************************************************

    Ocean Imports

*******************************************************************************/

private import ocean.io.select.EpollSelectDispatcher,
               ocean.io.select.event.IntervalClock,
               ocean.io.digest.Fnv1,
               ocean.util.log.SimpleLayout;

/*******************************************************************************

    Tango Imports

*******************************************************************************/

private import tango.core.Thread,
               tango.util.log.Log,
               tango.util.container.HashSet;

private import Integer = tango.text.convert.Integer;

/*******************************************************************************

        Abstract Test class creating a basic dht client instance

*******************************************************************************/

class Test
{    
    /***************************************************************************

        The epoll instance

    ***************************************************************************/

    protected EpollSelectDispatcher epoll;
    
    /***************************************************************************

        The DhtClient instance

    ***************************************************************************/

    protected alias ExtensibleDhtClient!(.DhtClient.Scheduler) DhtClient;

    protected DhtClient dht;

    /***************************************************************************

        Request Notification info struct

    ***************************************************************************/

    protected Exception exception;

    /***************************************************************************

        Logger instance

    ***************************************************************************/

    protected Logger logger;

    /***************************************************************************

        Constructor
        
        Params:
            connections = amount of connections to use
            config      = path to the xml configuration file

    ***************************************************************************/

    this ( size_t connections, char[] config )
    {
        this.epoll = new EpollSelectDispatcher;
        this.dht = new DhtClient(epoll, new DhtClient.Scheduler(epoll), connections);

        Exception exception = null;
        
        void done ( DhtClient.RequestContext, bool success ) 
        {
            if ( !success ) exception = this.exception;
        }
        
        with (this.dht)
        {
            addNodes(config);
            nodeHandshake(&done, &this.requestNotifier);
        }
        
        this.runRequest(exception);
    }

    /***************************************************************************

        Run all tests of this test class

    ***************************************************************************/

    abstract void run ( );
    
    protected:

    /***************************************************************************

        Runs the eventloop and handles any resulting errors

    ***************************************************************************/

    void runRequest ( ref Exception exception ) 
    {        
        this.epoll.eventLoop;
        
        if ( exception !is null ) throw exception;
        
        if ( this.exception !is null ) throw this.exception;        
    }

    /***************************************************************************

        Request notifier callback

    ***************************************************************************/

    void requestNotifier ( DhtClient.RequestNotification info )
    {
        logger.trace("Notify: command={}, type={}, succeeded={}", info.command, info.type, info.succeeded);
        if ( info.type == info.type.Finished && !info.succeeded )
        {
            if (info.exception !is null)
            {
                this.exception = info.exception;
            }
            else
            {
                this.exception = new Exception(info.message);
            }
        }
    }      
}