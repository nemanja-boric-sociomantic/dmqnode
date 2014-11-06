/*******************************************************************************

    Copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

    Connection handler for key-value nodes.

    Currently only the connection setup params and request resources classes are
    shared.

*******************************************************************************/

module swarmnodes.common.kvstore.connection.KVConnectionHandler;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.connection.ConnectionHandler : ConnectionSetupParams;

private import swarmnodes.common.kvstore.connection.SharedResources;

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

    Helper class to acquire and relinquish resources required by a request
    while it is handled. The resources are acquired from the shared
    resources instance which is passed to KVConnectionHandler's
    constructor (in the KVConnectionSetupParams instance). Acquired
    resources are automatically relinquished in the destructor.

    Note that it is assumed that each request will own at most one of each
    resource type (it is not possible, for example, to acquire two value
    buffers).

*******************************************************************************/

template KVRequestResources ( )
{
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
}

