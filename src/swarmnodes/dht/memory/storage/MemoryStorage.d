/*******************************************************************************

    Memory Channel Storage Engine

    copyright:      Copyright (c) 2013 sociomantic labs. All rights reserved

    authors:        Leandro Lucarella

    This module implements the DhtStorageEngine for a memory channel using
    Tokyo Cabinet as the real storage engine.

*******************************************************************************/

module swarmnodes.dht.memory.storage.MemoryStorage;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.dht.DhtConst;

private import swarmnodes.dht.common.storage.DhtStorageEngine;

private import swarmnodes.dht.common.storage.IStepIterator;

private import ocean.db.tokyocabinet.TokyoCabinetM;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("swarmnodes.dht.memory.storage.MemoryStorage");
}



/***************************************************************************

    Memory storage engine

***************************************************************************/

public class MemoryStorage : DhtStorageEngine
{
    /***********************************************************************

        Callback type used to delete channel files when the channel is removed.

        Params:
            id = name of the channel to remove

    ***********************************************************************/

    public alias void delegate ( char[] id ) DeleteChannelCb;

    private const DeleteChannelCb delete_channel;


    /***********************************************************************

        Tokyo Cabinet instance

    ***********************************************************************/

    private const TokyoCabinetM tokyo;


    /***********************************************************************

        Constructor.

        Params:
            id = identifier string for this instance
            hash_range = hash range for which this node is responsible
            bnum = memory storage channels bnum value
            delete_channel = callback used to delete channel files when the
                    channel is removed

    ***********************************************************************/

    public this ( char[] id, DhtHashRange hash_range, uint bnum,
        DeleteChannelCb delete_channel )
    {
        super(id, hash_range);

        this.delete_channel = delete_channel;

        if ( bnum == 0 )
        {
            this.tokyo = new TokyoCabinetM;
        }
        else
        {
            this.tokyo = new TokyoCabinetM(bnum);
        }
    }


    /***********************************************************************

        Puts a record into the database.

        Params:
            key        = record key
            value      = record value

        Returns:
            this instance

    ***********************************************************************/

    override public typeof(this) put ( char[] key, char[] value )
    {
        this.tokyo.put(key, value);

        super.listeners.trigger(Listeners.Listener.Code.DataReady, key);

        return this;
    }


    /***********************************************************************

       Get record

       Params:
           key   = key to lookup
           value = return buffer

       Returns:
           this instance

    ***********************************************************************/

    override public typeof(this) get ( char[] key, ref char[] value )
    {
        value.length = 0;

        this.tokyo.get(key, value);

        return this;
    }


    /***********************************************************************

        Tells whether a record exists

         Params:
            key = record key

        Returns:
             true if record exists or false itherwise

   ************************************************************************/

    override public bool exists ( char[] key )
    {
        return this.tokyo.exists(key);
    }


    /***********************************************************************

        Remove record

        Params:
            key = key of record to remove

        Returns:
            this instance

    ***********************************************************************/

    override public typeof(this) remove ( char[] key )
    {
        this.tokyo.remove(key);

        return this;
    }


    /***********************************************************************

        Initialises a step-by-step iterator over the keys of all records in
        the database.

        Params:
            iterator = iterator to initialise

    ***********************************************************************/

    override public typeof(this) getAll ( IStepIterator iterator )
    {
        iterator.getAll();

        return this;
    }


    /***********************************************************************

        Performs any actions needed to safely close a channel. In the case
        of the memory database, nothing needs to be done.

        (Called from IStorageChannels when removing a channel or shutting
        down the node. In the former case, the channel is clear()ed then
        close()d. In the latter case, the channel is only close()d.)

        Returns:
            this instance

    ***********************************************************************/

    public typeof(this) close ( )
    {
        return this;
    }


    /***********************************************************************

        Removes all records from database. We also move the dump file for this
        channel (if one has been written) to deleted/channel_name.tcm, in order
        to ensure that if the node is restarted the deleted channel will not be
        loaded again and restored!

        (Called from IStorageChannels when removing a channel.)

        Returns:
            this instance

    ***********************************************************************/

    public typeof(this) clear ( )
    {
        this.tokyo.clear();

        this.delete_channel(this.id);

        return this;
    }


    /***********************************************************************

        Returns:
            number of records stored

    ***********************************************************************/

    public ulong num_records ( )
    {
        return this.tokyo.numRecords();
    }


    /***********************************************************************

        Returns:
            number of records stored

    ***********************************************************************/

    public ulong num_bytes ( )
    {
        return this.tokyo.dbSize();
    }


    /***********************************************************************

        Gets the first key in the database.

        Params:
            key = key output

        Returns:
            this instance

    ***********************************************************************/

    private typeof(this) getFirstKey ( ref char[] key )
    {
        this.tokyo.getFirstKey(key);
        return this;
    }


    /***********************************************************************

        Gets the key of the record following the specified key.

        Note: "following" means the next key in the Memory storage, which is
        *not* necessarily the next key in numerical order.

        Params:
            last_key = key to iterate from
            key = key output

        Returns:
            this instance

    ***********************************************************************/

    private typeof(this) getNextKey ( char[] last_key, ref char[] key )
    {
        if ( !this.tokyo.getNextKey(last_key, key) )
        {
            key.length = 0;
        }
        return this;
    }
}


/***********************************************************************

    Memory storage engine iterator.

    You can reuse an instance to this class to iterate over different
    MemoryStorage instances as long as you "reset" the iteration by calling
    getAll() just after you called setStorage().

***********************************************************************/

public class MemoryStorageStepIterator : IStepIterator
{
    /*******************************************************************

        Reference to storage engine, set by setStorage() method.

    *******************************************************************/

    private MemoryStorage storage;


    /*******************************************************************

        Key of current record.

    *******************************************************************/

    private char[] current_key;


    /*******************************************************************

        Value of current record.

    *******************************************************************/

    private char[] current_value;


    /*******************************************************************

        Storage initialiser.

        Params:
            storage = storage engine to iterate over

    *******************************************************************/

    public void setStorage ( DhtStorageEngine storage )
    {
        this.storage = cast(MemoryStorage)storage;
    }


    /*******************************************************************

        Initialises the iterator to iterate over all records in the
        storage engine. The first key is queued up, ready to be fetched
        with the methods below.

    *******************************************************************/

    public void getAll ( )
    in
    {
        assert(this.storage, typeof(this).stringof ~ ".getAll: storage not set");
    }
    body
    {
        this.storage.getFirstKey(this.current_key);
    }


    /*******************************************************************

        Initialises the iterator to iterate over all records in the
        storage engine within the specified range of keys. Not supported
        by the memory storage engine.

        Params:
            min = string containing the hexadecimal key of the first
                record to iterate
            max = string containing the hexadecimal key of the last
                record to iterate

    *******************************************************************/

    public void getRange ( char[] min, char[] max )
    in
    {
        assert(this.storage, typeof(this).stringof ~ ".getRange: storage not set");
    }
    body
    {
        throw this.storage.not_implemented_exception(DhtConst.Command.E.GetRange);
    }


    /*******************************************************************

        Gets the key of the current record the iterator is pointing to.

        Returns:
            current key

    *******************************************************************/

    public char[] key ( )
    {
        return this.current_key;
    }


    /*******************************************************************

        Gets the value of the current record the iterator is pointing
        to.

        Returns:
            current value

    *******************************************************************/

    public char[] value ( )
    {
        this.storage.get(this.current_key, this.current_value);
        return this.current_value;
    }


    /*******************************************************************

        Advances the iterator to the next record.

    *******************************************************************/

    public void next ( )
    {
        this.storage.getNextKey(this.current_key, this.current_key);
    }
}

