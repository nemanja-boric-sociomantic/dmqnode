/*******************************************************************************

    Distributed Hashtable Node Connection Handler

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        December 2010: Initial release

    authors:        David Eckhardt, Gavin Norman

*******************************************************************************/

module swarmnodes.common.kvstore.connection.KVConnectionHandler;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.connection.ConnectionHandler;

private import swarm.core.node.model.INodeInfo;

private import swarm.dht.DhtConst;

private import swarmnodes.common.kvstore.node.IKVNodeInfo;

private import swarmnodes.common.kvstore.connection.SharedResources;

private import swarmnodes.common.kvstore.request.model.IRequest;
private import swarmnodes.common.kvstore.request.model.IKVRequestResources;

private import swarmnodes.common.kvstore.request.GetVersionRequest;
private import swarmnodes.common.kvstore.request.GetResponsibleRangeRequest;
private import swarmnodes.common.kvstore.request.GetSupportedCommandsRequest;
private import swarmnodes.common.kvstore.request.GetChannelsRequest;
private import swarmnodes.common.kvstore.request.GetSizeRequest;
private import swarmnodes.common.kvstore.request.GetChannelSizeRequest;
private import swarmnodes.common.kvstore.request.GetAllRequest;
private import swarmnodes.common.kvstore.request.GetAllFilterRequest;
private import swarmnodes.common.kvstore.request.GetAllKeysRequest;
private import swarmnodes.common.kvstore.request.RemoveChannelRequest;
private import swarmnodes.common.kvstore.request.GetNumConnectionsRequest;
private import swarmnodes.common.kvstore.request.RedistributeRequest;

private import swarmnodes.dht.request.ExistsRequest;
private import swarmnodes.dht.request.GetRequest;
private import swarmnodes.dht.request.PutRequest;
private import swarmnodes.dht.request.RemoveRequest;
private import swarmnodes.dht.request.ListenRequest;

private import swarmnodes.logfiles.request.PutDupRequest;
private import swarmnodes.logfiles.request.GetRangeRequest;
private import swarmnodes.logfiles.request.GetRangeFilterRequest;

private import swarmnodes.common.kvstore.storage.KVStorageChannels;



/*******************************************************************************

    Key/value node connection handler setup class. Passed to the key/value
    connection handler constructor.

    TODO: enable HMAC authentication by deriving from HmacAuthConnectionSetupParams

*******************************************************************************/

public class KVConnectionSetupParams : ConnectionSetupParams
{
    /***************************************************************************

        Reference to the storage channels which the requests are operating on.

    ***************************************************************************/

    public KVStorageChannels storage_channels;


    /***************************************************************************

        Reference to the request resources pool shared between all connection
        handlers.

    ***************************************************************************/

    public SharedResources shared_resources;
}



/*******************************************************************************

    Key/value connection handler class.

    An object pool of these connection handlers is contained in the
    SelectListener which is instantiated inside KVNode.

    TODO: enable HMAC authentication by deriving from HmacAuthConnectionHandler

*******************************************************************************/

public class KVConnectionHandler
    : ConnectionHandlerTemplate!(DhtConst.Command)
{
    /***************************************************************************

        Helper class to acquire and relinquish resources required by a request
        while it is handled. The resources are acquired from the shared
        resources instance which is passed to KVConnectionHandler's
        constructor (in the DhtConnectionSetupParams instance). Acquired
        resources are automatically relinquished in the destructor.

        Note that it is assumed that each request will own at most one of each
        resource type (it is not possible, for example, to acquire two value
        buffers).

    ***************************************************************************/

    private scope class KVRequestResources
        : RequestResources, IKVRequestResources
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

        public KVStorageChannels storage_channels ( )
        {
            return this.setup.storage_channels;
        }


        /***********************************************************************

            Node info getter.

        ***********************************************************************/

        public IKVNodeInfo node_info ( )
        {
            return cast(IKVNodeInfo)this.setup.node_info;
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

            RedistributeNode buffer newer.

        ***********************************************************************/

        protected RedistributeNode[] new_redistribute_node_buffer ( )
        {
            return new RedistributeNode[2];
        }


        /***********************************************************************

            Select event newer.

        ***********************************************************************/

        protected FiberSelectEvent new_event ( )
        {
            return new FiberSelectEvent(this.outer.fiber);
        }


        /***********************************************************************

            Select timer newer.

        ***********************************************************************/

        protected FiberTimerEvent new_timer ( )
        {
            return new FiberTimerEvent(this.outer.fiber);
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

            Dht client newer.

        ***********************************************************************/

        protected DhtClient new_dht_client ( )
        {
            return new DhtClient(this.outer.fiber.epoll);
        }


        /***********************************************************************

            Select event initialiser.

        ***********************************************************************/

        override protected void init_event ( FiberSelectEvent event )
        {
            event.fiber = this.outer.fiber;
        }


        /***********************************************************************

            Select timer initialiser.

        ***********************************************************************/

        override protected void init_timer ( FiberTimerEvent timer )
        {
            timer.fiber = this.timer.fiber;
        }


        /***********************************************************************

            Loop ceder initialiser.

        ***********************************************************************/

        override protected void init_loop_ceder ( LoopCeder loop_ceder )
        {
            loop_ceder.event = this.event;
        }


        /***********************************************************************

            Dht client initialiser.

        ***********************************************************************/

        override protected void init_dht_client ( DhtClient dht_client )
        {
            dht_client.clearNodes();
        }


        /***********************************************************************

            Returns:
                setup parameters for this connection handler

        ***********************************************************************/

        private KVConnectionSetupParams setup ( )
        {
            return cast(KVConnectionSetupParams)this.outer.setup;
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

        Command code 'Redistribute' handler.

    ***************************************************************************/

    protected void handleRedistribute ( )
    {
        this.handleCommand!(RedistributeRequest);
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
        scope resources = new KVRequestResources;
        scope handler = new Handler(this.reader, this.writer, resources);

        // calls handler.handle() and checks memory and buffer allocation after
        // request finishes
        this.handleRequest!(KVConnectionResources)(handler, resources);
    }
}

