/*******************************************************************************

    Queue Node Connection Handler

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module queuenode.connection.QueueConnectionHandler;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.connection.ConnectionHandler;
private import swarm.core.node.model.INodeInfo;

private import swarm.queue.QueueConst;

private import queueproto.node.request.model.QueueCommand;

private import queuenode.connection.SharedResources;

private import queuenode.request.model.IQueueRequestResources;

private import queuenode.request.PopRequest;
private import queuenode.request.PushRequest;
private import queuenode.request.ProduceRequest;
private import queuenode.request.ProduceMultiRequest;
private import queuenode.request.GetChannelsRequest;
private import queuenode.request.GetChannelSizeRequest;
private import queuenode.request.GetSizeRequest;
private import queuenode.request.GetSizeLimitRequest;
private import queuenode.request.GetNumConnectionsRequest;
private import queuenode.request.ConsumeRequest;
private import queuenode.request.PushMultiRequest;
private import queuenode.request.PushMulti2Request;
private import queuenode.request.RemoveChannelRequest;

private import queuenode.storage.model.QueueStorageChannels;



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

        Helper class adding a couple of queue-specific getters as well as the
        resource acquiring getters required by the QueueCommand protocol base
        class. The resources are acquired from the shared
        resources instance which is passed to QueueConnectionHandler's
        constructor (in the QueueConnectionSetupParams instance). Acquired
        resources are automatically relinquished in the destructor.

        Note that it is assumed that each request will own at most one of each
        resource type (it is not possible, for example, to acquire two value
        buffers).

    ***************************************************************************/

    private scope class QueueRequestResources
        : RequestResources, IQueueRequestResources, QueueCommand.Resources
    {
        /***********************************************************************

            Constructor.

        ***********************************************************************/

        public this ( )
        {
            super(this.setup.shared_resources);
        }

        /***********************************************************************

            Forwarding QueueCommand.Resources methods

        ***********************************************************************/

        override public char[]* getChannelBuffer ( )
        {
            return this.channel_buffer;
        }

        override public char[]* getValueBuffer ( )
        {
            return this.value_buffer;
        }

        override public StringListReader getChannelListReader ( )
        {
            return this.string_list_reader;
        }

        /***********************************************************************

            Storage channels getter.

        ***********************************************************************/

        override public QueueStorageChannels storage_channels ( )
        {
            return this.setup.storage_channels;
        }


        /***********************************************************************

            Node info getter.

        ***********************************************************************/

        override public IQueueNodeInfo node_info ( )
        {
            return cast(IQueueNodeInfo)this.setup.node_info;
        }


        /***********************************************************************

            Channel buffer newer.

        ***********************************************************************/

        override protected char[] new_channel_buffer ( )
        {
            return new char[10];
        }


        /***********************************************************************

            Channel flags buffer newer.

        ***********************************************************************/

        override protected bool[] new_channel_flags_buffer ( )
        {
            return new bool[5];
        }


        /***********************************************************************

            Value buffer newer.

        ***********************************************************************/

        override protected char[] new_value_buffer ( )
        {
            return new char[50];
        }

        /***********************************************************************

            Channel list buffer newer

        ***********************************************************************/

        protected char[][] new_channel_list_buffer ( )
        {
            return new char[][this.storage_channels.length];
        }

        /***********************************************************************

            Select event newer.

        ***********************************************************************/

        override protected FiberSelectEvent new_event ( )
        {
            return new FiberSelectEvent(this.outer.fiber);
        }


        /***********************************************************************

            String list reader newer.

            Note: the string list reader returned by this method also acquires
            and uses a channel buffer. It is thus not possible to use the
            channel buffer independently.

        ***********************************************************************/

        override protected StringListReader new_string_list_reader ( )
        {
            this.channel_buffer();
            return new StringListReader(this.outer.reader,
                this.acquired.channel_buffer);
        }


        /***********************************************************************

            Loop ceder newer.

        ***********************************************************************/

        override protected LoopCeder new_loop_ceder ( )
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

    override protected void handleNone ( )
    {
        this.handleInvalidCommand();
    }


    /***************************************************************************

        Command code 'Push' handler.

    ***************************************************************************/

    override protected void handlePush ( )
    {
        this.handleCommand!(PushRequest);
    }

    /***************************************************************************

        Command code 'Pop' handler.

    ***************************************************************************/

    override protected void handlePop ( )
    {
        this.handleCommand!(PopRequest);
    }


    /***************************************************************************

        Command code 'GetChannels' handler.

    ***************************************************************************/

    override protected void handleGetChannels ( )
    {
        this.handleCommand!(GetChannelsRequest);
    }


    /***************************************************************************

        Command code 'GetChannelSize' handler.

    ***************************************************************************/

    override protected void handleGetChannelSize ( )
    {
        this.handleCommand!(GetChannelSizeRequest);
    }


    /***************************************************************************

        Command code 'GetSize' handler.

    ***************************************************************************/

    override protected void handleGetSize ( )
    {
        this.handleCommand!(GetSizeRequest);
    }


    /***************************************************************************

        Command code 'Consume' handler.

    ***************************************************************************/

    override protected void handleConsume ( )
    {
        this.handleCommand!(ConsumeRequest);
    }


    /***************************************************************************

        Command code 'GetSizeLimit' handler.

    ***************************************************************************/

    override protected void handleGetSizeLimit ( )
    {
        this.handleCommand!(GetSizeLimitRequest);
    }


    /***************************************************************************

        Command code 'GetNumConnections' handler.

    ***************************************************************************/

    override protected void handleGetNumConnections ( )
    {
        this.handleCommand!(GetNumConnectionsRequest);
    }


    /***************************************************************************

        Command code 'PushMulti' handler.

    ***************************************************************************/

    override protected void handlePushMulti ( )
    {
        this.handleCommand!(PushMultiRequest);
    }


    /***************************************************************************

        Command code 'PushMulti2' handler.

    ***************************************************************************/

    override protected void handlePushMulti2 ( )
    {
        this.handleCommand!(PushMulti2Request);
    }


    /***************************************************************************

        Command code 'Produce' handler.

    ***************************************************************************/

    override protected void handleProduce ( )
    {
        this.handleCommand!(ProduceRequest);
    }


    /***************************************************************************

        Command code 'ProduceMulti' handler.

    ***************************************************************************/

    override protected void handleProduceMulti ( )
    {
        this.handleCommand!(ProduceMultiRequest);
    }


    /***************************************************************************

        Command code 'RemoveChannel' handler.

    ***************************************************************************/

    override protected void handleRemoveChannel ( )
    {
        this.handleCommand!(RemoveChannelRequest);
    }


    /***************************************************************************

        Command handler method template.

        Template params:
            Handler = type of request handler

    ***************************************************************************/

    private void handleCommand ( Handler : QueueCommand ) ( )
    {
        scope resources = new QueueRequestResources;
        scope handler = new Handler(this.reader, this.writer, resources);

        // calls handler.handle() and checks memory and buffer allocation after
        // request finishes
        this.handleRequest!(QueueConnectionResources)(handler, resources);
    }
}

