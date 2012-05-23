/*******************************************************************************

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        November 2010: Initial release
    
    authors:        Gavin Norman

    Static bool indicating that application termination has been requested. Used
    to synchronize termination of the queue node and the service threads.

*******************************************************************************/

module src.mod.node.util.Terminator;



struct Terminator
{
    static bool terminating;
}

