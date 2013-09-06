/*******************************************************************************

    Distributed Hashtable Node Connection Handler

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        December 2010: Initial release

    authors:        David Eckhardt, Gavin Norman

*******************************************************************************/

module src.mod.dht.DhtConnectionHandler;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.ConnectionHandler;

private import swarm.core.node.model.INodeInfo;

private import swarm.dht.DhtConst;

private import src.mod.dht.model.IDhtNodeInfo;

private import src.mod.dht.connection.SharedResources;

private import src.mod.dht.request.model.IRequest;
private import src.mod.dht.request.model.IDhtRequestResources;

private import src.mod.dht.request.GetVersionRequest;
private import src.mod.dht.request.GetResponsibleRangeRequest;
private import src.mod.dht.request.GetSupportedCommandsRequest;
private import src.mod.dht.request.GetChannelsRequest;
private import src.mod.dht.request.GetSizeRequest;
private import src.mod.dht.request.GetChannelSizeRequest;
private import src.mod.dht.request.PutRequest;
private import src.mod.dht.request.PutDupRequest;
private import src.mod.dht.request.GetRequest;
private import src.mod.dht.request.GetAllRequest;
private import src.mod.dht.request.GetAllFilterRequest;
private import src.mod.dht.request.GetAllKeysRequest;
private import src.mod.dht.request.GetRangeRequest;
private import src.mod.dht.request.GetRangeFilterRequest;
private import src.mod.dht.request.ListenRequest;
private import src.mod.dht.request.ExistsRequest;
private import src.mod.dht.request.RemoveRequest;
private import src.mod.dht.request.RemoveChannelRequest;
private import src.mod.dht.request.GetNumConnectionsRequest;

private import src.mod.dht.storage.model.DhtStorageChannels;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Dht node connection handler setup class. Passed to the dht connection
    handler constructor.

    TODO: enable HMAC authentication by deriving from HmacAuthConnectionSetupParams

*******************************************************************************/

public class DhtConnectionSetupParams : ConnectionSetupParams
{
    /***************************************************************************

        Reference to the storage channels which the requests are operating on.

    ***************************************************************************/

    public DhtStorageChannels storage_channels;


    /***************************************************************************

        Reference to the request resources pool shared between all connection
        handlers.

    ***************************************************************************/

    public SharedResources shared_resources;
}



/*******************************************************************************

    Dht connection handler class.

    An object pool of these connection handlers is contained in the
    SelectListener which is instantiated inside DhtNode.

    TODO: enable HMAC authentication by deriving from HmacAuthConnectionHandler

*******************************************************************************/

public class DhtConnectionHandler
    : ConnectionHandlerTemplate!(DhtConst.Command)
{
    /***************************************************************************

        Helper class to acquire and relinquish resources required by a request
        while it is handled. The resources are acquired from the shared
        resources instance which is passed to DhtConnectionHandler's
        constructor (in the DhtConnectionSetupParams instance). Acquired
        resources are automatically relinquished in the destructor.

        Note that it is assumed that each request will own at most one of each
        resource type (it is not possible, for example, to acquire two value
        buffers).

    ***************************************************************************/

    private scope class DhtRequestResources
        : RequestResources, IDhtRequestResources
    {
        /***********************************************************************

            Constructor.

        ***********************************************************************/

        public this ( )
        {
            super(this.setup.shared_resources);
        }


        /***********************************************************************

            Storage channels getter.

        ***********************************************************************/

        public DhtStorageChannels storage_channels ( )
        {
            return this.setup.storage_channels;
        }


        /***********************************************************************

            Node info getter.

        ***********************************************************************/

        public IDhtNodeInfo node_info ( )
        {
            return cast(IDhtNodeInfo)this.setup.node_info;
        }


        /***********************************************************************

            Channel buffer newer.

        ***********************************************************************/

        protected char[] new_channel_buffer ( )
        {
            return new char[32];
        }


        /***********************************************************************

            Key buffer newers.

        ***********************************************************************/

        protected char[] new_key_buffer ( )
        {
            return new char[16]; // 16 hex digits in a 64-bit hash
        }

        protected char[] new_key2_buffer ( )
        {
            return new char[16]; // 16 hex digits in a 64-bit hash
        }


        /***********************************************************************

            Value buffer newer.

        ***********************************************************************/

        protected char[] new_value_buffer ( )
        {
            return new char[512];
        }


        /***********************************************************************

            Filter buffer newer.

        ***********************************************************************/

        protected char[] new_filter_buffer ( )
        {
            return new char[10];
        }


        /***********************************************************************

            Batch buffer newer.

        ***********************************************************************/

        protected char[] new_batch_buffer ( )
        {
            return new char[RecordBatcher.MaxBatchSize];
        }


        /***********************************************************************

            Hash buffer newer.

        ***********************************************************************/

        protected hash_t[] new_hash_buffer ( )
        {
            return new hash_t[10];
        }


        /***********************************************************************

            Select event newer.

        ***********************************************************************/

        protected FiberSelectEvent new_event ( )
        {
            return new FiberSelectEvent(this.outer.fiber);
        }


        /***********************************************************************

            Step iterator newer.

        ***********************************************************************/

        protected IStepIterator new_iterator ( )
        {
            return this.setup.storage_channels.newIterator();
        }


        /***********************************************************************

            Loop ceder newer.

        ***********************************************************************/

        protected LoopCeder new_loop_ceder ( )
        {
            return new LoopCeder(this.event);
        }


        /***********************************************************************

            Record batcher newer.

        ***********************************************************************/

        protected RecordBatcher new_batcher ( )
        {
            return new RecordBatcher(this.setup.lzo.lzo);
        }


        /***********************************************************************

            Select event initialiser.

        ***********************************************************************/

        override protected void init_event ( FiberSelectEvent event )
        {
            event.fiber = this.outer.fiber;
        }


        /***********************************************************************

            Loop ceder initialiser.

        ***********************************************************************/

        override protected void init_loop_ceder ( LoopCeder loop_ceder )
        {
            loop_ceder.event = this.event;
        }


        /***********************************************************************

            Returns:
                setup parameters for this connection handler

        ***********************************************************************/

        private DhtConnectionSetupParams setup ( )
        {
            return cast(DhtConnectionSetupParams)this.outer.setup;
        }
    }


    /***************************************************************************

        Constructor.

        Params:
            finalize_dg = user-specified finalizer, called when the connection
                is shut down
            setup = struct containing setup data for this connection

    ***************************************************************************/

    public this ( FinalizeDg finalize_dg, ConnectionSetupParams setup )
    {
        super(finalize_dg, setup);
    }


    /***************************************************************************

        Command code 'None' handler. Treated the same as an invalid command
        code.

    ***************************************************************************/

    protected void handleNone ( )
    {
        this.handleInvalidCommand();
    }


    /***************************************************************************

        Command code 'GetVersion' handler.

    ***************************************************************************/

    protected void handleGetVersion ( )
    {
        this.handleCommand!(GetVersionRequest);
    }


    /***************************************************************************

        Command code 'GetResponsibleRange' handler.

    ***************************************************************************/

    protected void handleGetResponsibleRange ( )
    {
        this.handleCommand!(GetResponsibleRangeRequest);
    }


    /***************************************************************************

        Command code 'GetSupportedCommands' handler.

    ***************************************************************************/

    protected void handleGetSupportedCommands ( )
    {
        this.handleCommand!(GetSupportedCommandsRequest);
    }


    /***************************************************************************

        Command code 'GetNumConnections' handler.

    ***************************************************************************/

    protected void handleGetNumConnections ( )
    {
        this.handleCommand!(GetNumConnectionsRequest);
    }


    /***************************************************************************

        Command code 'GetChannels' handler.

    ***************************************************************************/

    protected void handleGetChannels ( )
    {
        this.handleCommand!(GetChannelsRequest);
    }


    /***************************************************************************

        Command code 'GetSize' handler.

    ***************************************************************************/

    protected void handleGetSize ( )
    {
        this.handleCommand!(GetSizeRequest);
    }


    /***************************************************************************

        Command code 'GetChannelSize' handler.

    ***************************************************************************/

    protected void handleGetChannelSize ( )
    {
        this.handleCommand!(GetChannelSizeRequest);
    }


    /***************************************************************************

        Command code 'Put' handler.

    ***************************************************************************/

    protected void handlePut ( )
    {
        this.handleCommand!(PutRequest);
    }


    /***************************************************************************

        Command code 'PutDup' handler.

    ***************************************************************************/

    protected void handlePutDup ( )
    {
        this.handleCommand!(PutDupRequest);
    }


    /***************************************************************************

        Command code 'Get' handler.

    ***************************************************************************/

    protected void handleGet ( )
    {
        this.handleCommand!(GetRequest);
    }


    /***************************************************************************

        Command code 'GetAll' handler.

    ***************************************************************************/

    protected void handleGetAll ( )
    {
        this.handleCommand!(GetAllRequest);
    }


    /***************************************************************************

        Command code 'GetAll2' handler.

    ***************************************************************************/

    protected void handleGetAll2 ( )
    {
        this.handleCommand!(GetAllRequest2);
    }


    /***************************************************************************

        Command code 'GetAllFilter' handler.

    ***************************************************************************/

    protected void handleGetAllFilter ( )
    {
        this.handleCommand!(GetAllFilterRequest);
    }


    /***************************************************************************

        Command code 'GetAllFilter2' handler.

    ***************************************************************************/

    protected void handleGetAllFilter2 ( )
    {
        this.handleCommand!(GetAllFilterRequest2);
    }


    /***************************************************************************

        Command code 'GetAllKeys' handler.

    ***************************************************************************/

    protected void handleGetAllKeys ( )
    {
        this.handleCommand!(GetAllKeysRequest);
    }


    /***************************************************************************

        Command code 'GetAllKeys2' handler.

    ***************************************************************************/

    protected void handleGetAllKeys2 ( )
    {
        this.handleCommand!(GetAllKeysRequest2);
    }


    /***************************************************************************

        Command code 'GetRange' handler.

    ***************************************************************************/

    protected void handleGetRange ( )
    {
        this.handleCommand!(GetRangeRequest);
    }


    /***************************************************************************

        Command code 'GetRange2' handler.

    ***************************************************************************/

    protected void handleGetRange2 ( )
    {
        this.handleCommand!(GetRangeRequest2);
    }


    /***************************************************************************

        Command code 'GetRangeFilter' handler.

    ***************************************************************************/

    protected void handleGetRangeFilter ( )
    {
        this.handleCommand!(GetRangeFilterRequest);
    }


    /***************************************************************************

        Command code 'GetRangeFilter2' handler.

    ***************************************************************************/

    protected void handleGetRangeFilter2 ( )
    {
        this.handleCommand!(GetRangeFilterRequest2);
    }


    /***************************************************************************

        Command code 'Listen' handler.

    ***************************************************************************/

    protected void handleListen ( )
    {
        this.handleCommand!(ListenRequest);
    }


    /***************************************************************************

        Command code 'Exists' handler.

    ***************************************************************************/

    protected void handleExists ( )
    {
        this.handleCommand!(ExistsRequest);
    }


    /***************************************************************************

        Command code 'Remove' handler.

    ***************************************************************************/

    protected void handleRemove ( )
    {
        this.handleCommand!(RemoveRequest);
    }


    /***************************************************************************

        Command code 'RemoveChannel' handler.

    ***************************************************************************/

    protected void handleRemoveChannel ( )
    {
        this.handleCommand!(RemoveChannelRequest);
    }


    /***************************************************************************

        Called when an invalid command code is read from the connection.

    ***************************************************************************/

    protected void handleInvalidCommand_ ( )
    {
        super.writer.write(DhtConst.Status.E.InvalidRequest);
    }


    /***************************************************************************

        Command handler method template.

        Template params:
            Handler = type of request handler

    ***************************************************************************/

    private void handleCommand ( Handler : IRequest ) ( )
    {
        scope resources = new DhtRequestResources;
        scope handler = new Handler(this.reader, this.writer, resources);
        handler.handle();
    }
}

