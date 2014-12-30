/*******************************************************************************

    Queue Storage engine abstract base class

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        October 2010: Initial release

    authors:        David Eckardt, Gavin Norman

    Description:

    The QueueStorageEngine abstract class is the base class for the storage engines
    used in the Queue Node.

    The queue storage engine extends the base storage engine with the following
    features:
        * Methods to push & pop data.
        * A set of consumers -- clients waiting to read data from the channel.
        * A method to register a new consumer with the channel.

*******************************************************************************/

module queuenode.queue.storage.model.QueueStorageEngine;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.storage.model.IStorageEngine;

private import swarm.core.node.storage.listeners.Listeners;

private import ocean.util.OceanException;

private import tango.io.FilePath;

private import tango.core.Array;

private import tango.io.Path : normalize, PathParser;

private import tango.sys.Environment;



public abstract class QueueStorageEngine : IStorageEngine
{
    /***************************************************************************

        Set of consumers waiting for data on this storage channel. When data
        arrives the next consumer in the set is notified (round robin). When a
        flush / finish signal for the channel is received, all registered
        consumers are notified.

    ***************************************************************************/

    protected static class Consumers : IListeners!()
    {
        public alias Listener.Code ListenerCode;

        protected void trigger_ ( Listener.Code code )
        {
            switch ( code )
            {
                case code.DataReady:
                    auto listener = this.listeners.next();
                    if ( listener )
                    {
                        listener.trigger(code);
                    }
                    break;

                case code.Flush:
                    super.trigger_(code);
                    break;
                case code.Finish:
                    super.trigger_(code);
                    break;
            }
        }
    }

    protected Consumers consumers;


    /***************************************************************************

        Alias Listener -> IConsumer

    ***************************************************************************/

    public alias Consumers.Listener IConsumer;


    /***************************************************************************

        Constructor

        Params:
            id    = identifier string for this instance

     **************************************************************************/

    protected this ( char[] id )
    {
        super(id);

        this.consumers = new Consumers;
    }


    /***************************************************************************

        Tells whether a record will fit in this queue.

        Params:
            value = record value

        Returns:
            true if the record could be pushed

     **************************************************************************/

    abstract public bool willFit ( char[] value );


    /***************************************************************************

        Pushes a record into queue, notifying any waiting consumers that data is
        ready.

        Params:
            value = record value

        Returns:
            true if the record was pushed

     **************************************************************************/

    public bool push ( char[] value )
    {
        bool pushed;

        if ( this.willFit(value) )
        {
            pushed = this.push_(value);

            if ( pushed )
            {
                this.consumers.trigger(Consumers.ListenerCode.DataReady);
            }
        }

        return pushed;
    }


    /***************************************************************************

        Reset method, called when the storage engine is returned to the pool in
        IStorageChannels. Sends the Finish trigger to all registered consumers,
        which will cause the requests to end (as the channel being consumed is
        now gone).

    ***************************************************************************/

    public override void reset ( )
    {
        this.consumers.trigger(Consumers.ListenerCode.Finish);
    }


    /***************************************************************************

        Flushes sending data buffers of consumer connections.

    ***************************************************************************/

    public override void flush ( )
    {
        this.consumers.trigger(Consumers.ListenerCode.Flush);
    }


    /***************************************************************************

        Attempts to push a record into queue.

        Params:
            value = record value

        Returns:
            true if the record was pushed

     **************************************************************************/

    abstract protected bool push_ ( char[] value );


    /***************************************************************************

        Pops a record from queue.

        Params:
            value = record value

        Returns:
            this instance

     **************************************************************************/

    abstract public typeof(this) pop ( ref char[] value );


    /***************************************************************************

        Registers a consumer with the channel. The dataReady() method of the
        given consumer may be called when data is put to the channel.

        Params:
            consumer = consumer to notify when data is ready

    ***************************************************************************/

    public void registerConsumer ( IConsumer consumer )
    {
        this.consumers.register(consumer);
    }


    /***************************************************************************

        Unregisters a consumer from the channel.

        Params:
            consumer = consumer to stop notifying when data is ready

    ***************************************************************************/

    public void unregisterConsumer ( IConsumer consumer )
    {
        this.consumers.unregister(consumer);
    }
}

