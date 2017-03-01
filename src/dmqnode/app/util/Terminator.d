/*******************************************************************************

    Namespace struct containing a global boolean which is set to true when the
    application is terminating (after receiving SIGINT - see IKVNode).

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

*******************************************************************************/

module dmqnode.app.util.Terminator;


struct Terminator
{
    static bool terminating;

    // FIXME: see FIXME in dmqnode.dht.core.model.IKVNode, nodeError()
    static bool shutdown;
}
