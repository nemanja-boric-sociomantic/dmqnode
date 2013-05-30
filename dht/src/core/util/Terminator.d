/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        15/06/2012: Initial release

    authors:        Gavin Norman

    Namespace struct containing a global boolean which is set to true when the
    application is terminating (after receiving SIGINT - see IDhtNode).

*******************************************************************************/

module src.core.util.Terminator;



struct Terminator
{
    static bool terminating;

    // FIXME: see FIXME in src.core.model.IDhtNode, nodeError()
    static bool shutdown;
}

