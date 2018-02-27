/*******************************************************************************

    Table of request handlers by command.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.node.RequestHandlers;

import dmqnode.request.neo.Consume;
import dmqnode.request.neo.Pop;
import dmqnode.request.neo.Push;

import swarm.neo.node.ConnectionHandler;
import swarm.neo.request.Command;

import dmqproto.common.RequestCodes;

/*******************************************************************************

    This table of request handlers by command is used by the connection handler.
    When creating a new request, the function corresponding to the request
    command is called in a fiber.

*******************************************************************************/

public ConnectionHandler.RequestMap request_handlers;

static this ( )
{
    request_handlers.add(Command(RequestCode.Consume, 3), "consume",
        ConsumeImpl_v3.classinfo, false);
    request_handlers.add(Command(RequestCode.Push, 2), "push",
        PushImpl_v2.classinfo);
    request_handlers.add(Command(RequestCode.Pop, 0), "pop",
        PopImpl_v0.classinfo);
}
