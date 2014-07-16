/*******************************************************************************

    DhtNode storage channel registry

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        January 2010    initial release
                    August 2010     revised version

    authors:        David Eckardt, Thomas Nicolai, Gavin Norman

    Extends the core storage channels base class with the following features:
        * A method to tell whether a given command (from DhtConst.Command) is
          supported by the storage channels.
        * A method to get the total size in bytes required by the storage engine
          to store a set of concatenated values (i.e. PutCat).

*******************************************************************************/

module swarmnodes.dht.common.storage.DhtStorageChannels;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.dht.common.node.DhtHashRange;

private import ocean.core.Array;

private import swarm.core.node.storage.model.IStorageChannels;

private import swarmnodes.dht.common.storage.DhtStorageEngine;
private import swarmnodes.dht.common.storage.IStepIterator;

private import swarm.dht.DhtConst;
private import swarm.dht.DhtHash;

private import ocean.io.FilePath;

private import tango.sys.Environment;

private import PathUtil = tango.io.Path : normalize;



/*******************************************************************************

    StorageChannels class.

*******************************************************************************/

abstract public class DhtStorageChannels :
    IStorageChannelsTemplate!(DhtStorageEngine)
{
    /***************************************************************************

        Aliases for derived classes.

    ***************************************************************************/

    protected alias .DhtHashRange DhtHashRange;


    /***************************************************************************

        Storage data directory (copied in constructor)

    ***************************************************************************/

    protected const FilePath dir;


    /***************************************************************************

        Minimum and maximum record hashes supported by node

    ***************************************************************************/

    protected const DhtHashRange hash_range;


    /***************************************************************************

        Constructor. Creates storage data directory if it doesn't already exist.

        The channels should be loaded (loadChannels()) by the children classes
        after everything is properly initialised.

        Params:
            dir = storage data directory
            size_limit = maximum number of bytes allowed in the node (0 = no
                limit)
            hash_range = hash range for which this node is responsible

    ***************************************************************************/

    public this ( char[] dir, ulong size_limit, DhtHashRange hash_range )
    {
        super(size_limit);

        this.dir = this.getWorkingPath(dir);

        if ( !this.dir.exists )
        {
            this.createWorkingDir();
        }

        this.hash_range = hash_range;
    }


    /***************************************************************************

        Creates a new instance of an iterator for a storage engine.

        Returns:
            new iterator

    ***************************************************************************/

    abstract public IStepIterator newIterator ( );


    /***************************************************************************

        Tells whether a command is supported by this set of storage channels.

        Params:
            cmd = command to check

        Returns:
            true if command is supported

    ***************************************************************************/

    abstract public bool commandSupported ( DhtConst.Command.E cmd );


    /***************************************************************************

        Changes the hash range of the node. All storage engines have a reference
        to the same NodeHashRange instance, so will be updated immediately.

        Params:
            min = new min hash
            max = new max hash

        Throws:
            if the specified range is invalid

    ***************************************************************************/

    public void setHashRange ( hash_t min, hash_t max )
    {
        this.hash_range.set(min, max);
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
        return DhtHash.isWithinNodeResponsibility(hash, this.hash_range.range.min,
            this.hash_range.range.max);
    }


    /***************************************************************************

        Initialises storage channels (usually scanning this.dir).

    ***************************************************************************/

    abstract protected void loadChannels ( );


    /***************************************************************************

        Generates a absolute, normalized path string from path.

        Params:
            path = file path

        Returns:
            absolute, normalized path string

    ***************************************************************************/

    protected char[] getFullPathString ( FilePath path )
    {
        return path.set(normalize(path.folder)).toString; // TODO: probably a slight memory leak (but only called in ctor)
    }


    /***************************************************************************

        Creates a FilePath instance set to the absolute path of dir, if dir is
        not null, or to the current working directory of the environment
        otherwise.

        Params:
            dir = directory string; null indicates that the current working
                  directory of the environment should be used

        Returns:
            FilePath instance holding path

    ***************************************************************************/

    private FilePath getWorkingPath ( char[] dir )
    {
        FilePath path = new FilePath;

        if ( dir )
        {
            path.set(dir);

            if ( !path.isAbsolute() )
            {
                path.prepend(Environment.cwd());
            }
        }
        else
        {
            path.set(Environment.cwd());
        }

        return path;
    }


    /***************************************************************************

        Creates data directory.

    ***************************************************************************/

    private void createWorkingDir ( )
    {
        try
        {
            this.dir.createFolder();
        }
        catch (Exception e)
        {
            e.msg = typeof(this).stringof ~ ": Failed creating directory: " ~ e.msg;

            throw e;
        }
    }
}

