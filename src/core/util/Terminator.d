/*******************************************************************************

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        November 2010: Initial release
    
    authors:        Gavin Norman

    Namespace struct containing a global boolean which is set to true when the
    application is terminating (after receiving SIGINT - see QueueNodeServer).

*******************************************************************************/

module src.core.util.Terminator;



struct Terminator
{
    static bool terminating;
}

