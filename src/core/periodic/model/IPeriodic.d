/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        17/02/2012: Initial release
                    30/05/30: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

    Abstract base class for periodics -- routines which should be invoked
    periodically using an epoll timer.

*******************************************************************************/

module src.core.periodic.model.IPeriodic;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.io.select.event.TimerEvent;

private import ocean.io.select.EpollSelectDispatcher;

private import src.core.util.Terminator;

private import swarm.core.node.model.INode;
private import swarm.core.node.model.INodeInfo;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger logger;
static this ( )
{
    logger = Log.lookup("src.core.periodic.model.IPeriodic");
}



/*******************************************************************************

    Periodic base class.

*******************************************************************************/

public abstract class IPeriodic : TimerEvent
{
    /***************************************************************************

        Local alias redefinitions for derived classes

    ***************************************************************************/

    protected alias .INode INode;

    protected alias .INodeInfo INodeInfo;

    /***************************************************************************

        Interface to a swarm node.

    ***************************************************************************/

    protected INode node;


    /***************************************************************************

        Identifying string of this periodic, used for logging.

    ***************************************************************************/

    private const char[] id;


    /***************************************************************************

        Constructor.

        Params:
            period_ms = milliseconds between calls to handle()
            id = identifying string of this periodic, used for logging

    ***************************************************************************/

    public this ( uint period_ms, char[] id )
    {
        super(&this.handle);

        auto s = period_ms / 1000;
        auto ms = (period_ms) % 1000;
        this.set(s, ms, s, ms);

        this.id = id;
    }


    /***************************************************************************

        Passes a swarm node interface to the periodic. The interface is passed via
        this method rather than the constructor so that a single instance of the
        class Periodics can be instantiated and passed a swarm node interface
        which it will in turn pass through to all IPeriodic instances which it
        owns.

        Params:
            node = swarm node interface to use

    ***************************************************************************/

    public void setNode ( INode node )
    {
        this.node = node;
    }


    /***************************************************************************

        Timer callback. If the termination (SIGINT) signal has not been
        received, then the abstract handle_() method is called and the periodic
        is registered back with epoll to fire again. If termination has been
        requested, then the method does nothing and simply returns false to
        unregister from epoll.

    ***************************************************************************/

    private bool handle ( )
    {
        assert(this.node !is null);

        if ( !Terminator.terminating )
        {
            this.handle_();
        }

        return !Terminator.terminating; // unless terminating, always stay registered to fire again periodically
    }


    /***************************************************************************

        Abstract handle method which should implement the desired behaviour of
        the periodic.

    ***************************************************************************/

    protected abstract void handle_ ( );


    /***************************************************************************

        Error reporting method, called when an Exception is caught from
        handle().

        Params:
            exception: Exception thrown by handle()
            event:     Seletor event while exception was caught

    ***************************************************************************/

    override protected void error_ ( Exception exception, Event event )
    {
        logger.error("Exception caught in timer handler for {}: {} @ {}:{}",
            this.id, exception.msg, exception.file, exception.line);
    }
}

