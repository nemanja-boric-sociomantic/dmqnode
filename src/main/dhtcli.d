/*******************************************************************************

    DHT command-line client

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        October 2011: Initial release

    authors:        Leandro Lucarella

    See src.mod.cli.DhtCli documentation for details.

*******************************************************************************/

module src.main.dhtcli;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.cli.DhtCli;

private import ocean.text.Arguments;



/*******************************************************************************

    Main

    Params:
        arguments = command line arguments

*******************************************************************************/

void main ( char[][] arguments )
{
    auto app_name = arguments[0];

    scope args = new Arguments;
    scope app = new DhtCli;

    if ( app.parseArgs(app_name, args, arguments[1..$]) )
    {
        app.run(args);
    }
}

