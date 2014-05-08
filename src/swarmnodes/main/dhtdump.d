/*******************************************************************************

    Copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

    Dht node channel dump tool.

*******************************************************************************/

module swarmnodes.main.dhtdump;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.mod.dhtdump.DhtDump;



/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts dht node.

    Params:
        cl_args = array with raw command line arguments

*******************************************************************************/

private int main ( char[][] cl_args )
{
    auto app = new DhtDump;
    return app.main(cl_args);
}

