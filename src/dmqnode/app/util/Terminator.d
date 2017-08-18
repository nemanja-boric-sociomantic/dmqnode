/*******************************************************************************

    Namespace struct containing a global boolean which is set to true when the
    application is terminating (after receiving SIGINT - see IKVNode).

    copyright:
        Copyright (c) 2012-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.app.util.Terminator;


struct Terminator
{
    static bool terminating;

    // FIXME: see FIXME in dmqnode.dht.core.model.IKVNode, nodeError()
    static bool shutdown;
}
