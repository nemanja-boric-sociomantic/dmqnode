/*******************************************************************************

    Queue Node Connection Handler

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module swarmnodes.queue.connection.QueueConnectionHandler;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.connection.ConnectionHandler;

private import swarm.core.node.model.INodeInfo;

private import swarm.queue.QueueConst;

private import swarmnodes.queue.connection.SharedResources;

private import swarmnodes.queue.request.model.IRequest;
private import swarmnodes.queue.request.model.IQueueRequestResources;

private import swarmnodes.queue.request.PopRequest;
private import swarmnodes.queue.request.PushRequest;
private import swarmnodes.queue.request.ProduceRequest;
private import swarmnodes.queue.request.ProduceMultiRequest;
private import swarmnodes.queue.request.GetChannelsRequest;
private import swarmnodes.queue.request.GetChannelSizeRequest;
private import swarmnodes.queue.request.GetSizeRequest;
private import swarmnodes.queue.request.GetSizeLimitRequest;
private import swarmnodes.queue.request.GetNumConnectionsRequest;
private import swarmnodes.queue.request.ConsumeRequest;
private import swarmnodes.queue.request.PushMultiRequest;
private import swarmnodes.queue.request.RemoveChannelRequest;

private import swarmnodes.queue.storage.model.QueueStorageChannels;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Queue node connection handler setup class. Passed to the queue connection
    handler constructor.

    TODO: enable HMAC authentication by deriving from HmacAuthConnectionSetupParams

*******************************************************************************/

public class QueueConnectionSetupParams : ConnectionSetupParams
{
    /***************************************************************************

        Reference to the storage channels which the requests are operating on.

    ***************************************************************************/

    public QueueStorageChannels storage_channels;


    /***************************************************************************

        Reference to the request resources pool shared between all connection
        handlers.

    ***************************************************************************/

    public SharedResources shared_resources;
}



/*******************************************************************************

    Queue node connection handler class.

    An object pool of these connection handlers is contained in the
    SelectListener which is instantiated inside the QueueNode.

    TODO: enable HMAC authentication by deriving from HmacAuthConnectionHandler

*******************************************************************************/

public class QueueConnectionHandler
    : ConnectionHandlerTemplate!(QueueConst.Command)
{
    /***************************************************************************

        Helper class to acquire and relinquish resources required by a request
        while it is handled. The resources are acquired from the shared
        resources instance which is passed to QueueConnectionHandler's
        constructor (in the QueueConnectionSetupParams instance). Acquired
        resources are automatically relinquished in the destructor.

        Note that it is assumed that each request will own at most one of each
        resource type (it is not possible, for example, to acquire two value
        buffers).

    ***************************************************************************/

    private scope class QueueRequestResources
        : RequestResources, IQueueRequestResources
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

        public QueueStorageChannels storage_channels ( )
        {
            return this.setup.storage_channels;
        }


        /***********************************************************************

            Node info getter.

        ***********************************************************************/

        public IQueueNodeInfo node_info ( )
        {
            return cast(IQueueNodeInfo)this.setup.node_info;
        }


        /***********************************************************************

            Channel buffer newer.

        ***********************************************************************/

        protected char[] new_channel_buffer ( )
        {
            return new char[10];
        }


        /***********************************************************************

            Channel flags buffer newer.

        ***********************************************************************/

        protected bool[] new_channel_flags_buffer ( )
        {
            return new bool[5];
        }


        /***********************************************************************

            Value buffer newer.

        ***********************************************************************/

        protected char[] new_value_buffer ( )
        {
            return new char[50];
        }


        /***********************************************************************

            Select event newer.

        ***********************************************************************/

        protected FiberSelectEvent new_event ( )
        {
            return new FiberSelectEvent(this.outer.fiber);
        }


        /***********************************************************************

            String list reader newer.

            Note: the string list reader returned by this method also acquires
            and uses a channel buffer. It is thus not possible to use the
            channel buffer independently.

        ***********************************************************************/

        protected StringListReader new_string_list_reader ( )
        {
            this.channel_buffer();
            return new StringListReader(this.outer.reader,
                this.acquired.channel_buffer);
        }


        /***********************************************************************

            Loop ceder newer.

        ***********************************************************************/

        protected LoopCeder new_loop_ceder ( )
        {
            return new LoopCeder(this.event);
        }


        /***********************************************************************

            Select event initialiser.

        ***********************************************************************/

        override protected void init_event ( FiberSelectEvent event )
        {
            event.fiber = this.outer.fiber;
        }


        /***********************************************************************

            String list reader initialiser.

            Note: the string list reader returned by this method also acquires
            and uses a channel buffer. It is thus not possible to use the
            channel buffer independently.

        ***********************************************************************/

        override protected void init_string_list_reader ( StringListReader
            string_list_reader )
        {
            this.channel_buffer();
            string_list_reader.reinitialise(this.outer.reader,
                &this.acquired.channel_buffer);
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

        private QueueConnectionSetupParams setup ( )
        {
            return cast(QueueConnectionSetupParams)this.outer.setup;
        }
    }


    /***************************************************************************

        Constructor.

        Params:
            finalize_dg = user-specified finalizer, called when the connection
                is shut down
            setup = struct containing everything needed to set up a connection

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

        Command code 'Push' handler.

    ***************************************************************************/

    protected void handlePush ( )
    {
        this.handleCommand!(PushRequest);
    }

    /***************************************************************************

        Command code 'Pop' handler.

    ***************************************************************************/

    protected void handlePop ( )
    {
        this.handleCommand!(PopRequest);
    }


    /***************************************************************************

        Command code 'GetChannels' handler.

    ***************************************************************************/

    protected void handleGetChannels ( )
    {
        this.handleCommand!(GetChannelsRequest);
    }


    /***************************************************************************

        Command code 'GetChannelSize' handler.

    ***************************************************************************/

    protected void handleGetChannelSize ( )
    {
        this.handleCommand!(GetChannelSizeRequest);
    }


    /***************************************************************************

        Command code 'GetSize' handler.

    ***************************************************************************/

    protected void handleGetSize ( )
    {
        this.handleCommand!(GetSizeRequest);
    }


    /***************************************************************************

        Command code 'Consume' handler.

    ***************************************************************************/

    protected void handleConsume ( )
    {
        this.handleCommand!(ConsumeRequest);
    }


    /***************************************************************************

        Command code 'GetSizeLimit' handler.

    ***************************************************************************/

    protected void handleGetSizeLimit ( )
    {
        this.handleCommand!(GetSizeLimitRequest);
    }


    /***************************************************************************

        Command code 'GetNumConnections' handler.

    ***************************************************************************/

    protected void handleGetNumConnections ( )
    {
        this.handleCommand!(GetNumConnectionsRequest);
    }


    /***************************************************************************

        Command code 'PushMulti' handler.

    ***************************************************************************/

    protected void handlePushMulti ( )
    {
        this.handleCommand!(PushMultiRequest);
    }


    /***************************************************************************

        Command code 'Produce' handler.

    ***************************************************************************/

    protected void handleProduce ( )
    {
        this.handleCommand!(ProduceRequest);
    }


    /***************************************************************************

        Command code 'ProduceMulti' handler.

    ***************************************************************************/

    protected void handleProduceMulti ( )
    {
        this.handleCommand!(ProduceMultiRequest);
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
        super.writer.write(QueueConst.Status.E.InvalidRequest);
    }


    /***************************************************************************

        Command handler method template.

        Template params:
            Handler = type of request handler

    ***************************************************************************/

    private void handleCommand ( Handler : IRequest ) ( )
    {
        scope resources = new QueueRequestResources;
        scope handler = new Handler(this.reader, this.writer, resources);

        // calls handler.handle() and checks memory and buffer allocation after
        // request finishes
        this.handleRequest!(QueueConnectionResources)(handler, resources);
    }
}

