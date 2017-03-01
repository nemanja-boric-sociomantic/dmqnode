/*******************************************************************************

    copyright: Copyright (c) 2016 sociomantic labs. All rights reserved

    Table of request handlers by command.

*******************************************************************************/

module dmqnode.node.RequestHandlers;

import swarm.core.neo.node.ConnectionHandler;

import swarm.dmq.DmqConst;

import Consume = dmqnode.request.neo.Consume;
import Push    = dmqnode.request.neo.Push;
import Pop     = dmqnode.request.neo.Pop;

/*******************************************************************************

    This table of request handlers by command is used by the connection handler.
    When creating a new request, the function corresponding to the request
    command is called in a fiber.

*******************************************************************************/

public ConnectionHandler.CmdHandlers request_handlers;

static this ( )
{
    request_handlers[DmqConst.Command.E.Consume] = &Consume.handle;
    request_handlers[DmqConst.Command.E.Push]    = &Push.handle;
    request_handlers[DmqConst.Command.E.Pop]     = &Pop.handle;
}
