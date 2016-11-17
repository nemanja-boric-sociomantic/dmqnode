/*******************************************************************************

    Pop request implementation.

    copyright: Copyright (c) 2016 sociomantic labs. All rights reserved

*******************************************************************************/

module dmqnode.request.neo.Pop;

import dmqproto.node.neo.request.Pop;

import dmqnode.connection.neo.SharedResources;
import dmqnode.storage.model.StorageEngine;

import swarm.core.neo.node.ConnectionHandler;
import dmqproto.common.Pop;
import swarm.core.neo.request.Command;

import ocean.transition;
import ocean.core.TypeConvert : downcast;

/*******************************************************************************

    The request handler for the table of handlers. When called, runs in a fiber
    that can be controlled via `connection`.

    Params:
        shared_resources = an opaque object containing resources owned by the
            node which are required by the request
        connection  = performs connection socket I/O and manages the fiber
        cmdver      = the version number of the Consume command as specified by
                      the client
        msg_payload = the payload of the first message of this request

*******************************************************************************/

void handle (
    Object shared_resources,
    ConnectionHandler.RequestOnConn connection,
    ConnectionHandler.Command.Version cmdver,
    void[] msg_payload
)
{
    auto dmq_shared_resources = downcast!(SharedResources)(shared_resources);
    assert(dmq_shared_resources);

    switch ( cmdver )
    {
        case 0:
            scope rq_resources = dmq_shared_resources.new RequestResources;
            scope rq = new PopImpl_v0(rq_resources);
            rq.handle(connection, msg_payload);
            break;

        default:
            auto ed = connection.event_dispatcher;
            ed.send(
                ( ed.Payload payload )
                {
                    payload.addConstant(GlobalStatusCode.RequestVersionNotSupported);
                }
            );
            break;
    }
}

/*******************************************************************************

    DMQ node implementation of the v0 Pop request protocol.

*******************************************************************************/

private scope class PopImpl_v0 : PopProtocol_v0
{
    import ocean.core.TypeConvert : castFrom, downcast;

    /***************************************************************************

        Shared resources acquirer passed to the constructor.

    ***************************************************************************/

    private SharedResources.RequestResources resources;

    /***************************************************************************

        Storage engine being popped from.

    ***************************************************************************/

    private StorageEngine storage_engine;

    /***************************************************************************

        Constructor.

        Params:
            resources = shared resources acquirer for the request

    ***************************************************************************/

    public this ( SharedResources.RequestResources resources )
    {
        super(resources);

        this.resources = resources;
    }

    /***************************************************************************

        Performs any logic needed to pop from the channel of the given name.

        Params:
            channel_name = channel to pop from

        Returns:
            `true` if the channel may be used

    ***************************************************************************/

    override protected bool prepareChannel ( cstring channel_name )
    {
        this.storage_engine =
            this.resources.storage_channels.getCreate(channel_name);

        return this.storage_engine !is null;
    }

    /***************************************************************************

        Pop the next value from the channel, if available.

        Params:
            value = buffer to write the value into

        Returns:
            `true` if there was a value in the channel, false if the channel is
            empty

    ***************************************************************************/

    override protected bool getNextValue ( ref void[] value )
    {
        auto mstring_value = castFrom!(void[]*).to!(mstring*)(&value);
        this.storage_engine.pop(*mstring_value);

        return value.length > 0;
    }
}
