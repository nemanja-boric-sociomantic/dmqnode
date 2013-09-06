/*******************************************************************************

    GetAllKeys request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        August 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.dht.request.GetAllKeysRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.dht.request.model.IBulkGetRequest;

private import swarm.dht.common.RecordBatcher;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    GetAllKeys request

*******************************************************************************/

private scope class IGetAllKeysRequest ( bool ChunkedBatcher, DhtConst.Command.E Cmd )
    : IBulkGetRequest!(ChunkedBatcher)
{
    /***************************************************************************

        Constructor
    
        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        IDhtRequestResources resources )
    {
        super(Cmd, reader, writer, resources);
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
    }


    /***************************************************************************

        Initiates iteration over the specified channel using the specified
        iterator.

        Params:
            storage = storage channel to iterate over
            iterator = iterator instance to use

    ***************************************************************************/

    protected void beginIteration_ ( DhtStorageEngine storage_channel,
        IStepIterator iterator )
    {
        storage_channel.getAll(iterator);
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
        add_result = super.addToBatch(super.key);
        return true;
    }
}

public alias IGetAllKeysRequest!(true, DhtConst.Command.E.GetAllKeys) GetAllKeysRequest;

public alias IGetAllKeysRequest!(false, DhtConst.Command.E.GetAllKeys2) GetAllKeysRequest2;

