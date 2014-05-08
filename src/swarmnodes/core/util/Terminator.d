/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        15/06/2012: Initial release
                    30/05/2013: Combined dht and queue project

    authors:        Gavin Norman

    Namespace struct containing a global boolean which is set to true when the
    application is terminating (after receiving SIGINT - see IDhtNode).

*******************************************************************************/

module swarmnodes.core.util.Terminator;



struct Terminator
{
    static bool terminating;

    // FIXME: see FIXME in swarmnodes.dht.core.model.IDhtNode, nodeError()
    static bool shutdown;
}

