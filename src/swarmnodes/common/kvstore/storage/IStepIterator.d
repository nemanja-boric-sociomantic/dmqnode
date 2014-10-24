/*******************************************************************************

    Abstract base class for key/value node step-by-step iterators. An iterator
    class must be implemented for each storage engine.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        January 2011: Initial release

    authors:        Gavin Norman

    A step iterator is distinguished from an opApply style iterator in that it
    has explicit methods to get the current key / value, and to advance the
    iterator to the next key. This type of iterator is essential for an
    asynchronous storage engine, as multiple iterations could be occurring in
    parallel (asynchronously), and each one needs to be able to remember its own
    state (ie which record it's up to, and which is next). This class provides
    the interface for that kind of iterator.

    This abstract iterator class has no methods to begin an iteration. As
    various different types of iteration are possible, it is left to derived
    classes to implement suitable methods to start iterations.

*******************************************************************************/

module swarmnodes.common.kvstore.storage.IStepIterator;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.common.kvstore.storage.KVStorageEngine;



abstract public class IStepIterator
{
    /***************************************************************************

        Sets the storage engine over which to iterate.

        Params:
            storage = iteration storage engine

    ***************************************************************************/

    abstract public void setStorage ( KVStorageEngine storage );


    /***************************************************************************

        Gets the key of the current record the iterator is pointing to.

        Returns:
            current key

    ***************************************************************************/

    abstract public char[] key ( );


    /***************************************************************************

        Gets the value of the current record the iterator is pointing to.

        Returns:
            current value

    ***************************************************************************/

    abstract public char[] value ( );


    /***************************************************************************

        Advances the iterator to the next record.

    ***************************************************************************/

    abstract public void next ( );


    /***************************************************************************

        Tells whether the current record pointed to by the iterator is the last
        in the iteration.

        This method may be overridden, but the default definition of the
        iteration end is that the current key is empty.

        Returns:
            true if the current record is the last in the iteration

    ***************************************************************************/

    public bool lastKey ( )
    {
        return this.key.length == 0;
    }


    /***************************************************************************

        Performs any required de-initialisation behaviour. Base class does
        nothing, but derived classes can override this method to add their own
        behaviour.

    ***************************************************************************/

    public void finished ( )
    {
    }
}

