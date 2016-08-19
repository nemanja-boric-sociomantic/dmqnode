/*******************************************************************************

    copyright: Copyright (c) 2016 sociomantic labs. All rights reserved

    Push request implementation.

*******************************************************************************/

module dmqnode.request.neo.Push;

import dmqnode.connection.neo.SharedResources;
import swarm.core.neo.node.ConnectionHandler;
import swarm.dmq.DmqConst;
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

    auto ed     = connection.event_dispatcher,
         parser = ed.message_parser;

    char[] channel_name;
    void[] value;

    parser.parseBody(msg_payload, channel_name, value);

    if (auto storage_channel =
        dmq_shared_resources.storage_channels.getCreate(channel_name))
    {
        storage_channel.push(cast(char[])value);
        ed.sendT(DmqConst.Status.E.Ok);
    }
    else
    {
        ed.sendT(DmqConst.Status.E.Error);
    }
}
