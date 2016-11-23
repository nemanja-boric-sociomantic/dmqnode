/*******************************************************************************

    Distributed Message Queue Node Connection Handler

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module dmqnode.connection.ConnectionHandler;



/*******************************************************************************

    Imports

*******************************************************************************/

private import Swarm = swarm.core.node.connection.ConnectionHandler;
private import swarm.core.node.model.INodeInfo;

private import swarm.dmq.DmqConst;

private import dmqproto.node.request.model.DmqCommand;

private import dmqnode.connection.SharedResources;

private import dmqnode.request.model.IDmqRequestResources;

private import dmqnode.request.PopRequest;
private import dmqnode.request.PushRequest;
private import dmqnode.request.ProduceRequest;
private import dmqnode.request.ProduceMultiRequest;
private import dmqnode.request.GetChannelsRequest;
private import dmqnode.request.GetChannelSizeRequest;
private import dmqnode.request.GetSizeRequest;
private import dmqnode.request.GetNumConnectionsRequest;
private import dmqnode.request.ConsumeRequest;
private import dmqnode.request.PushMultiRequest;
private import dmqnode.request.RemoveChannelRequest;

private import dmqnode.storage.model.StorageChannels;



/*******************************************************************************

    DMQ node connection handler setup class. Passed to the DMQ connection
    handler constructor.

    TODO: enable HMAC authentication by deriving from HmacAuthConnectionSetupParams

*******************************************************************************/

public class ConnectionSetupParams : Swarm.ConnectionSetupParams
{
    /***************************************************************************

        Reference to the storage channels which the requests are operating on.

    ***************************************************************************/

    public StorageChannels storage_channels;


    /***************************************************************************

        Reference to the request resources pool shared between all connection
        handlers.

    ***************************************************************************/

    public SharedResources shared_resources;
}



/*******************************************************************************

    DMQ node connection handler class.

    An object pool of these connection handlers is contained in the
    SelectListener which is instantiated inside the DmqNode.

    TODO: enable HMAC authentication by deriving from HmacAuthConnectionHandler

*******************************************************************************/

public class ConnectionHandler
    : Swarm.ConnectionHandlerTemplate!(DmqConst.Command)
{
    /***************************************************************************

        Helper class adding a couple of DMQ-specific getters as well as the
        resource acquiring getters required by the DmqCommand protocol base
        class. The resources are acquired from the shared
        resources instance which is passed to ConnectionHandler's
        constructor (in the ConnectionSetupParams instance). Acquired
        resources are automatically relinquished in the destructor.

        Note that it is assumed that each request will own at most one of each
        resource type (it is not possible, for example, to acquire two value
        buffers).

    ***************************************************************************/

    private scope class DmqRequestResources
        : RequestResources, IDmqRequestResources, DmqCommand.Resources
    {
        /***********************************************************************

            Constructor.

        ***********************************************************************/

        public this ( )
        {
            super(this.setup.shared_resources);
        }

        /***********************************************************************

            Forwarding DmqCommand.Resources methods

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

        override public StorageChannels storage_channels ( )
        {
            return this.setup.storage_channels;
        }


        /***********************************************************************

            Node info getter.

        ***********************************************************************/

        override public IDmqNodeInfo node_info ( )
        {
            return cast(IDmqNodeInfo)this.setup.node_info;
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

        override protected char[][] new_channel_list_buffer ( )
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

        private ConnectionSetupParams setup ( )
        {
            return cast(ConnectionSetupParams)this.outer.setup;
        }
    }


    /***************************************************************************

        Constructor.

        Params:
            finalize_dg = user-specified finalizer, called when the connection
                is shut down
            setup = struct containing everything needed to set up a connection

    ***************************************************************************/

    public this ( FinalizeDg finalize_dg, Swarm.ConnectionSetupParams setup )
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
        this.handleInvalidCommand();
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
        this.handleInvalidCommand();
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

    private void handleCommand ( Handler : DmqCommand ) ( )
    {
        scope resources = new DmqRequestResources;
        scope handler = new Handler(this.reader, this.writer, resources);

        // calls handler.handle() and checks memory and buffer allocation after
        // request finishes
        this.handleRequest!(ConnectionResources)(handler, resources);
    }
}

