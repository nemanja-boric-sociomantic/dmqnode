/*******************************************************************************

    GetRangeFilter request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        August 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module queuenode.logfiles.request.GetRangeFilterRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import queuenode.common.kvstore.request.model.IBulkGetRequest;

private import swarm.dht.common.RecordBatcher;

private import tango.text.Search;



/*******************************************************************************

    GetRangeFilter request

*******************************************************************************/

private scope class GetRangeFilterRequest : IBulkGetRequest
{
    /***************************************************************************

        Sub-string search instance.

    ***************************************************************************/

    SearchFruct!(char) match;


    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        IKVRequestResources resources )
    {
        super(DhtConst.Command.E.GetRangeFilter, reader, writer, resources);
    }


    /***************************************************************************

        Reads any data from the client which is required for the request. If the
        request is invalid in some way (the channel name is invalid, or the
        command is not supported) then the command can be simply not executed,
        and all client data has been read, leaving the read buffer in a clean
        state ready for the next request.

    ***************************************************************************/

    protected void readRequestData_ ( )
    {
        this.reader.readArray(*this.resources.key_buffer);
        this.reader.readArray(*this.resources.key2_buffer);
        this.reader.readArray(*this.resources.filter_buffer);

        this.match = search(*this.resources.filter_buffer);
    }


    /***************************************************************************

        Initiates iteration over the specified channel using the specified
        iterator.

        Params:
            storage = storage channel to iterate over
            iterator = iterator instance to use

    ***************************************************************************/

    protected void beginIteration_ ( KVStorageEngine storage_channel, IStepIterator iterator )
    {
        storage_channel.getRange(iterator, *this.resources.key_buffer,
            *this.resources.key2_buffer);
    }


    /***************************************************************************

        Called by getBatch() when a record is retrieved from the storage engine.
        Should add the record to the batch if desired (using the super class'
        addToBatch() methods), and return true in this case.

        Params:
            add_result = out value which receives a code indicating if the
                record was successfully added, and if not, why not

        Returns:
            true if the record was added

    ***************************************************************************/

    protected bool handleRecord ( out AddResult add_result )
    {
        if ( super.key >= *this.resources.key_buffer &&
             super.key <= *this.resources.key2_buffer )
        {
            auto value = super.value;
            if ( this.match.forward(value) < value.length )
            {
                add_result = super.addToBatch(super.key, value);
                return true;
            }
        }

        return false;
    }
}



