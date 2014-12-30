/*******************************************************************************

    Distributed Hashtable Storage engine abstract base class

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2010: Initial release
                    January 2011: Asynchronous version

    authors:        Lars Kirchhoff, Thomas Nicolai, David Eckardt, Gavin Norman

    The KVStorageEngine abstract class is the base class for the storage engines
    used in the Distributed Hashtable Node.

    This class contains methods which implement all the dht commands which
    operate over a storage engine (that is, excluding commands such as
    GetNumConnections and GetChannels, which are handled at a higher level).

    All command methods throw a NotImplementedException in the base class. To
    enable support for the command in a subclass, simply override these methods.

*******************************************************************************/

module queuenode.common.kvstore.storage.KVStorageEngine;



/*******************************************************************************

    Imports

*******************************************************************************/

private import queuenode.common.kvstore.node.KVHashRange;

private import swarm.core.node.storage.model.IStorageEngine;

private import queuenode.common.kvstore.storage.IStepIterator;

private import Hash = swarm.core.Hash;

private import swarm.dht.DhtConst;

private import ocean.core.Array : copy, append;



/*******************************************************************************

    Abstract storage class

*******************************************************************************/

abstract public class KVStorageEngine : IStorageEngine
{
    /***************************************************************************

        Aliases for derived classes.

    ***************************************************************************/

    protected alias .KVHashRange KVHashRange;


    /***************************************************************************

        Exception instance thrown when receiving an unimplemented command.

    ***************************************************************************/

    protected NotImplementedException not_implemented_exception;


    /***************************************************************************

        Minimum and maximum record hashes supported by node

    ***************************************************************************/

    protected const KVHashRange hash_range;


    /***************************************************************************

        Constructor

        Params:
            id = identifier string for this instance
            hash_range = hash range for which this node is responsible

     **************************************************************************/

    protected this ( char[] id, KVHashRange hash_range )
    {
        super(id);

        this.not_implemented_exception = new NotImplementedException;

        this.hash_range = hash_range;
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
        auto hash = Hash.straightToHash(key);
        return Hash.isWithinNodeResponsibility(hash, this.hash_range.range.min,
            this.hash_range.range.max);
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

