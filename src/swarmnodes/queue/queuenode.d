/*******************************************************************************

    Queue Node Server

    copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved

    version:        Jun 2009: Initial release
                    May 2013: Combined dht and queue project

    authors:        Gavin Norman, Hans Bjerkander

*******************************************************************************/

module swarmnodes.main.queuenode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.queue.QueueNodeServer;



/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts queue node.

    Params:
        cl_args = array with raw command line arguments

*******************************************************************************/

private int main ( char[][] cl_args )
{
    auto app = new QueueNodeServer;
    return app.main(cl_args);
}

