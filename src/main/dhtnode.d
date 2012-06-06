/*******************************************************************************

    Dht Node Server

    copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved

    version:        Jun 2009: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.main.dhtnode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.node.DhtNode;



/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts dht node.

    Params:
        cl_args = array with raw command line arguments

*******************************************************************************/

private int main ( char[][] cl_args )
{
    auto app = new DhtNodeServer;
    return app.main(cl_args);
}

