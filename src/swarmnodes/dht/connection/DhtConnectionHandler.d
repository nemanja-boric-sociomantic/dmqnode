/*******************************************************************************

    Distributed Hashtable Node Connection Handler

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        December 2010: Initial release

    authors:        David Eckhardt, Gavin Norman

*******************************************************************************/

module swarmnodes.dht.connection.DhtConnectionHandler;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.connection.ConnectionHandler;

private import swarm.core.node.model.INodeInfo;

private import swarm.dht.DhtConst;

private import swarmnodes.common.kvstore.node.IKVNodeInfo;

private import swarmnodes.common.kvstore.connection.KVConnectionHandler;
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
private import swarmnodes.common.kvstore.request.RemoveChannelRequest;
private import swarmnodes.common.kvstore.request.GetNumConnectionsRequest;

private import swarmnodes.dht.request.ExistsRequest;
private import swarmnodes.dht.request.GetRequest;
private import swarmnodes.dht.request.PutRequest;
private import swarmnodes.dht.request.RemoveRequest;
private import swarmnodes.dht.request.ListenRequest;
private import swarmnodes.dht.request.GetAllKeysRequest;
private import swarmnodes.dht.request.RedistributeRequest;

private import swarmnodes.common.kvstore.storage.KVStorageChannels;



/*******************************************************************************

    Dht node connection handler class.

    An object pool of these connection handlers is contained in the
    SelectListener which is instantiated inside KVNode.

    TODO: enable HMAC authentication by deriving from HmacAuthConnectionHandler

*******************************************************************************/

public class DhtConnectionHandler
    : ConnectionHandlerTemplate!(DhtConst.Command)
{
    /***************************************************************************

        Helper class to acquire and relinquish resources required by a request
        while it is handled. The resources are acquired from the shared
        resources instance which is passed to KVConnectionHandler's
        constructor (in the KVConnectionSetupParams instance). Acquired
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

        Reuseable exception thrown when the command code read from the client
        is not supported (i.e. does not have a corresponding entry in
        this.requests).

    ***************************************************************************/

    private Exception invalid_command_exception;


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

        this.invalid_command_exception = new Exception("Invalid command",
            __FILE__, __LINE__);
    }


    /***************************************************************************

        Command code 'None' handler. Treated the same as an invalid command
        code.

    ***************************************************************************/

    override protected void handleNone ( )
    {
        this.handleInvalidCommand();
    }


    /***************************************************************************

        Command code 'GetVersion' handler.

    ***************************************************************************/

    override protected void handleGetVersion ( )
    {
        this.handleCommand!(GetVersionRequest);
    }


    /***************************************************************************

        Command code 'GetResponsibleRange' handler.

    ***************************************************************************/

    override protected void handleGetResponsibleRange ( )
    {
        this.handleCommand!(GetResponsibleRangeRequest);
    }


    /***************************************************************************

        Command code 'GetSupportedCommands' handler.

    ***************************************************************************/

    override protected void handleGetSupportedCommands ( )
    {
        this.handleCommand!(GetSupportedCommandsRequest);
    }


    /***************************************************************************

        Command code 'GetNumConnections' handler.

    ***************************************************************************/

    override protected void handleGetNumConnections ( )
    {
        this.handleCommand!(GetNumConnectionsRequest);
    }


    /***************************************************************************

        Command code 'GetChannels' handler.

    ***************************************************************************/

    override protected void handleGetChannels ( )
    {
        this.handleCommand!(GetChannelsRequest);
    }


    /***************************************************************************

        Command code 'GetSize' handler.

    ***************************************************************************/

    override protected void handleGetSize ( )
    {
        this.handleCommand!(GetSizeRequest);
    }


    /***************************************************************************

        Command code 'GetChannelSize' handler.

    ***************************************************************************/

    override protected void handleGetChannelSize ( )
    {
        this.handleCommand!(GetChannelSizeRequest);
    }


    /***************************************************************************

        Command code 'Put' handler.

    ***************************************************************************/

    override protected void handlePut ( )
    {
        this.handleCommand!(PutRequest);
    }


    /***************************************************************************

        Command code 'PutDup' handler.

    ***************************************************************************/

    override protected void handlePutDup ( )
    {
        // TODO: remove this method when this command is removed from the list
        // of codes
        throw this.invalid_command_exception;
    }


    /***************************************************************************

        Command code 'Get' handler.

    ***************************************************************************/

    override protected void handleGet ( )
    {
        this.handleCommand!(GetRequest);
    }


    /***************************************************************************

        Command code 'GetAll' handler.

    ***************************************************************************/

    override protected void handleGetAll ( )
    {
        this.handleCommand!(GetAllRequest);
    }


    /***************************************************************************

        Command code 'GetAll2' handler.

    ***************************************************************************/

    override protected void handleGetAll2 ( )
    {
        this.handleCommand!(GetAllRequest2);
    }


    /***************************************************************************

        Command code 'GetAllFilter' handler.

    ***************************************************************************/

    override protected void handleGetAllFilter ( )
    {
        this.handleCommand!(GetAllFilterRequest);
    }


    /***************************************************************************

        Command code 'GetAllFilter2' handler.

    ***************************************************************************/

    override protected void handleGetAllFilter2 ( )
    {
        this.handleCommand!(GetAllFilterRequest2);
    }


    /***************************************************************************

        Command code 'GetAllKeys' handler.

    ***************************************************************************/

    override protected void handleGetAllKeys ( )
    {
        this.handleCommand!(GetAllKeysRequest);
    }


    /***************************************************************************

        Command code 'GetAllKeys2' handler.

    ***************************************************************************/

    override protected void handleGetAllKeys2 ( )
    {
        this.handleCommand!(GetAllKeysRequest2);
    }


    /***************************************************************************

        Command code 'GetRange' handler.

    ***************************************************************************/

    override protected void handleGetRange ( )
    {
        // TODO: remove this method when this command is removed from the list
        // of codes
        throw this.invalid_command_exception;
    }


    /***************************************************************************

        Command code 'GetRange2' handler.

    ***************************************************************************/

    override protected void handleGetRange2 ( )
    {
        // TODO: remove this method when this command is removed from the list
        // of codes
        throw this.invalid_command_exception;
    }


    /***************************************************************************

        Command code 'GetRangeFilter' handler.

    ***************************************************************************/

    override protected void handleGetRangeFilter ( )
    {
        // TODO: remove this method when this command is removed from the list
        // of codes
        throw this.invalid_command_exception;
    }


    /***************************************************************************

        Command code 'GetRangeFilter2' handler.

    ***************************************************************************/

    override protected void handleGetRangeFilter2 ( )
    {
        // TODO: remove this method when this command is removed from the list
        // of codes
        throw this.invalid_command_exception;
    }


    /***************************************************************************

        Command code 'Listen' handler.

    ***************************************************************************/

    override protected void handleListen ( )
    {
        this.handleCommand!(ListenRequest);
    }


    /***************************************************************************

        Command code 'Exists' handler.

    ***************************************************************************/

    override protected void handleExists ( )
    {
        this.handleCommand!(ExistsRequest);
    }


    /***************************************************************************

        Command code 'Remove' handler.

    ***************************************************************************/

    override protected void handleRemove ( )
    {
        this.handleCommand!(RemoveRequest);
    }


    /***************************************************************************

        Command code 'RemoveChannel' handler.

    ***************************************************************************/

    override protected void handleRemoveChannel ( )
    {
        this.handleCommand!(RemoveChannelRequest);
    }


    /***************************************************************************

        Command code 'Redistribute' handler.

    ***************************************************************************/

    override protected void handleRedistribute ( )
    {
        this.handleCommand!(RedistributeRequest);
    }


    /***************************************************************************

        Called when an invalid command code is read from the connection.

    ***************************************************************************/

    override protected void handleInvalidCommand_ ( )
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

