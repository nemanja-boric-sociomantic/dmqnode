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

module src.mod.dht.storage.model.DhtStorageChannels;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.core.Array;

private import swarm.core.node.storage.model.IStorageChannels;

private import src.mod.dht.storage.model.DhtStorageEngine;
private import src.mod.dht.storage.model.IStepIterator;

private import swarm.dht.DhtConst;

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

        Storage data directory (copied in constructor)

    ***************************************************************************/

    protected const FilePath dir;


    /***************************************************************************

        Minimum and maximum record hashes supported by node

    ***************************************************************************/

    protected const hash_t min_hash, max_hash;


    /***************************************************************************

        Constructor. Creates storage data directory if it doesn't already exist.

        The channels should be loaded (loadChannels()) by the children classes
        after everything is properly initialised.

        Params:
            dir = storage data directory
            size_limit = maximum number of bytes allowed in the node (0 = no
                limit)
            min_hash = minimum hash for which this node is responsible
            max_hash = maximum hash for which this node is responsible

    ***************************************************************************/

    public this ( char[] dir, ulong size_limit, hash_t min_hash, hash_t max_hash )
    {
        super(size_limit);

        this.dir = this.getWorkingPath(dir);

        if ( !this.dir.exists )
        {
            this.createWorkingDir();
        }

        this.min_hash = min_hash;
        this.max_hash = max_hash;
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

        Params:
            dir = directory to initialize; set to null to use the
                current working directory

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

