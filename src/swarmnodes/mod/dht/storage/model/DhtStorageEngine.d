/*******************************************************************************

    Distributed Hashtable Storage engine abstract base class

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2010: Initial release
                    January 2011: Asynchronous version

    authors:        Lars Kirchhoff, Thomas Nicolai, David Eckardt, Gavin Norman

    The DhtStorageEngine abstract class is the base class for the storage engines
    used in the Distributed Hashtable Node.

    This class contains methods which implement all the dht commands which
    operate over a storage engine (that is, excluding commands such as
    GetNumConnections and GetChannels, which are handled at a higher level).

    All command methods throw a NotImplementedException in the base class. To
    enable support for the command in a subclass, simply override these methods.

*******************************************************************************/

module swarmnodes.mod.dht.storage.model.DhtStorageEngine;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.storage.model.IStorageEngine;

private import swarm.core.node.storage.listeners.Listeners;

private import swarmnodes.mod.dht.storage.model.IStepIterator;

private import swarm.dht.DhtConst;
private import swarm.dht.DhtHash;

private import ocean.core.Array : copy, append;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Abstract storage class

*******************************************************************************/

abstract public class DhtStorageEngine : IStorageEngine
{
    /***************************************************************************

        Set of listeners waiting for data on this storage channel. When data
        arrives (or a flush / finish signal for the channel), all registered
        listeners are notified.

    ***************************************************************************/

    protected alias IListeners!(char[]) Listeners;

    protected Listeners listeners;


    /***************************************************************************

        Alias for a listener.

    ***************************************************************************/

    public alias Listeners.Listener IListener;


    /***************************************************************************

        Exception instance thrown when receiving an unimplemented command.

    ***************************************************************************/

    protected NotImplementedException not_implemented_exception;


    /***************************************************************************

        Minimum and maximum record hashes supported by node

    ***************************************************************************/

    protected const hash_t min_hash, max_hash;


    /***************************************************************************

        Constructor

        Params:
            id = identifier string for this instance
            min_hash = minimum hash for which this node is responsible
            max_hash = maximum hash for which this node is responsible

     **************************************************************************/

    protected this ( char[] id, hash_t min_hash, hash_t max_hash )
    {
        super(id);

        this.listeners = new Listeners;

        this.not_implemented_exception = new NotImplementedException;

        this.min_hash = min_hash;
        this.max_hash = max_hash;
    }


    /***************************************************************************

        Checks whether the specified key string (expected to be a hex number) is
        within the hash range of this storage engine.

        Params:
            key = record key

        Returns:
            true if the key is within the storage engine's hash range

    ***************************************************************************/

    public bool responsibleForKey ( char[] key )
    {
        auto hash = DhtHash.straightToHash(key);
        return hash >= this.min_hash && hash <= this.max_hash;
    }


    /***************************************************************************

        Puts a record into the database. If a record with the same key already
        exists, it is replaced.

        Params:
            key        = record key
            value      = record value

        Returns:
            this instance

     **************************************************************************/

    public typeof(this) put ( char[] key, char[] value )
    {
        throw this.not_implemented_exception(DhtConst.Command.E.Put);

        return this;
    }


    /***************************************************************************

        Puts a record into the database, allowing duplication.

        Note: This method does not have to be implemented; implementation is
        done by overriding this method.

        Params:
            key        = record key
            value      = record value

        Returns:
            this instance

     **************************************************************************/

    public typeof(this) putDup ( char[] key, char[] value )
    {
        throw this.not_implemented_exception(DhtConst.Command.E.PutDup);

        return this;
    }


    /***************************************************************************

        Gets a record from the database. If the requested record does not
        exist, value remains an empty string.

        Params:
            key        = record key
            value      = record value (output)

        Returns:
            this instance

     **************************************************************************/

    public typeof(this) get ( char[] key, ref char[] value )
    {
        throw this.not_implemented_exception(DhtConst.Command.E.Get);

        return this;
    }


    /***************************************************************************

        Initialises a step-by-step iterator over the keys of all records in the
        database.

        Params:
            iterator = iterator to initialise

     **************************************************************************/

    public typeof(this) getAll ( IStepIterator iterator )
    {
        throw this.not_implemented_exception(DhtConst.Command.E.GetAll);

        return this;
    }


    /***************************************************************************

        Initialises a step-by-step iterator over the keys of all records in the
        database in the specified range.

        Params:
            iterator = iterator to initialise
            min = minimum hash to iterate over
            max = maximum hash to iterate over

     **************************************************************************/

    public typeof(this) getRange ( IStepIterator iterator, char[] min, char[] max )
    {
        throw this.not_implemented_exception(DhtConst.Command.E.GetRange);

        return this;
    }


    /***************************************************************************

        Tells whether a record exists

         Params:
            key = record key

        Returns:
             true if record exists or false itherwise

     **************************************************************************/

    public bool exists ( char[] key )
    {
        throw this.not_implemented_exception(DhtConst.Command.E.Exists);

        return false;
    }


    /***************************************************************************

        Removes a record from the database.

        Params:
            key        = record key

        Returns:
            this instance

     **************************************************************************/

    public typeof(this) remove ( char[] key )
    {
        throw this.not_implemented_exception(DhtConst.Command.E.Remove);

        return this;
    }


    /***************************************************************************

        Reset method, called when the storage engine is returned to the pool in
        IStorageChannels. Sends the Finish trigger to all registered listeners,
        which will cause the requests to end (as the channel being listened to
        is now gone).

    ***************************************************************************/

    public override void reset ( )
    {
        this.listeners.trigger(IListener.Code.Finish, "");
    }


    /***************************************************************************

        Flushes sending data buffers of consumer connections.

    ***************************************************************************/

    public override void flush ( )
    {
        this.listeners.trigger(IListener.Code.Flush, "");
    }


    /***************************************************************************

        Registers a listener with the channel. The dataReady() method of the
        given listener will be called when data is put to the channel.

        Params:
            listener = listener to notify when data is ready

     **************************************************************************/

    public void registerListener ( IListener listener )
    {
        this.listeners.register(listener);
    }


    /***************************************************************************

        Unregisters a listener from the channel.

        Params:
            listener = listener to stop notifying when data is ready

     **************************************************************************/

    public void unregisterListener ( IListener listener )
    {
        this.listeners.unregister(listener);
    }


    /***************************************************************************

        Not implemented exception

        The exception is usually thrown if a type of request is not supported
        by the used storage engine.

    **************************************************************************/

    static public class NotImplementedException : Exception
    {
        this ( )
        {
            super("");
        }

        typeof(this) opCall ( DhtConst.Command.E cmd )
        {
            const msg = "request not implemented";

            auto cmd_descr = cmd in DhtConst.Command();
            if ( cmd_descr !is null )
            {
                super.msg.append(*cmd_descr, " ", msg);
            }
            else
            {
                super.msg.copy(msg);
            }

            return this;
        }
    }
}

