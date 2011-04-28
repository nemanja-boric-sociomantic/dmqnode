/*******************************************************************************

    DestinationQueue 

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        October 2010: Initial release

    authors:        Gavin Norman

    --

    Queue of records to write to the destination dht - builds up a set of
    records to be sent all at once to the destination dht.

    As the records are put() to the destination queue, the values are copied
    into an internal array. This means that the using class does not need to
    take care of managing its own pool of record values.

 ******************************************************************************/

module core.dht.DestinationQueue;



/*******************************************************************************

    Imports

*******************************************************************************/

private import  core.dht.DestinationQueue;

private import  ocean.core.Array;

private import  ocean.text.Arguments;

private import  swarm.dht.DhtClient,
                swarm.dht.DhtHash,
                swarm.dht.DhtConst;


/*******************************************************************************

     DestinationQueue class 


 ******************************************************************************/

class DestinationQueue
{
    /***************************************************************************
    
        Number of items stored in the queue before all are sent to the dht
    
    ***************************************************************************/
    
    private const QueueSize = 200;
    
    /***************************************************************************
    
        Stored records
    
    ***************************************************************************/
    
    private char[][QueueSize] records;
    
    /***************************************************************************
    
        Number of records in queue
    
    ***************************************************************************/
    
    private size_t count;
    
    /***************************************************************************
    
        Destination dht client reference (set in constructor)
    
    ***************************************************************************/
    
    private DhtClient dst;
    
    /***************************************************************************
    
        Channel to write to
    
    ***************************************************************************/
    
    private char[] channel;
    
    /***************************************************************************
    
        Start and end hash of range being copied - any records outside of
        this range which are attempted to be added to the queue will not be
        stored or copied
    
    ***************************************************************************/
    
    private hash_t start = hash_t.min;
    
    private hash_t end = hash_t.max;
    
    /***************************************************************************
    
        Whether to write with compression
    
    ***************************************************************************/
    
    private bool compress;
    
    /***************************************************************************
    
        Constructor.
        
        Params:
            dst = destination dht client
    
    ***************************************************************************/
    
    public this ( DhtClient dst )
    {
        this.dst = dst;
        this.count = 0;
    }
    
    /***************************************************************************
    
        Sets the channel to write to.
        
        Params:
            channel = channel name
    
    ***************************************************************************/
    
    public void setChannel ( char[] channel )
    {
        this.channel.copy(channel);
    }
    
    /***************************************************************************
    
        Sets the hash range to copy
        
        Params:
            start = start of range
            end = end of range
    
    ***************************************************************************/
    
    public void setRange ( hash_t start, hash_t end )
    {
        this.start = start;
        this.end = end;
    }
    
    /***************************************************************************
    
        Sets the compression flag.
        
        Params:
            compress = true to compress data to the destination dht
    
    ***************************************************************************/
    
    public void setCompression ( bool compress )
    {
        this.compress = compress;
    }
    
    /***************************************************************************
    
        Returns:
            the compression setting.
    
    ***************************************************************************/
    
    public bool compression ( )
    {
        return this.compress;
    }
    
    /***************************************************************************
    
        Puts a record into the queue. If the record's hash is outside the
        hash range set, the record is not added to the queue. After adding a
        record, if the queue is full then it is flushed (all records are
        put to the destination dht.)
        
        Params:
            key = key of record to put
            value = record value
    
    ***************************************************************************/
    
    public void put ( hash_t key, char[] value )
    in
    {
        assert(this.dst, typeof(this).stringof ~  ".put - cannot put before initialisation");
    }
    body
    {
        if ( key >= this.start && key <= this.end && value.length )
        {
            this.records[this.count].copy(value);
    
            if ( this.dst.commandSupported(DhtConst.Command.Put) )
            {
                if ( this.compress )
                {
                    this.dst.putCompressed(this.channel, key, &this.putRecord, DhtClient.RequestContext(this.count));
                }
                else
                {
                    this.dst.put(this.channel, key, &this.putRecord, DhtClient.RequestContext(this.count));
                }
            }
            else if ( this.dst.commandSupported(DhtConst.Command.PutDup) )
            {
                if ( this.compress )
                {
                    this.dst.putDupCompressed(this.channel, key, &this.putRecord, DhtClient.RequestContext(this.count));
                }
                else
                {
                    this.dst.putDup(this.channel, key, &this.putRecord, DhtClient.RequestContext(this.count));
                }
            }
            else
            {
                assert(false, typeof(this).stringof ~  ".put - neither Put nor PutDup supported by destination dht");
            }
    
            if ( ++this.count >= QueueSize )
            {
                this.flush();
            }
        }
    }

    /***************************************************************************
    
        Sends all queued records to the destination dht.
        
    ***************************************************************************/
    
    public void flush ( )
    in
    {
        assert(this.dst, typeof(this).stringof ~  ".flush - cannot flush before initialisation");
    }
    body
    {
        this.dst.eventLoop();
        this.count = 0;
    }


    /***************************************************************************
    
        Callback delegate for dht put requests. Provides the record to be put.

        Params:
            context = context of dht request (in this case stores the index into
                the local records array).

        Returns:
            record to be put to the dht

    ***************************************************************************/

    private char[] putRecord ( DhtClient.RequestContext context )
    {
        return this.records[context.integer];
    }
}

