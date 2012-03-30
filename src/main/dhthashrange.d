/*******************************************************************************

    Tool to calculate dht hash ranges

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        November 2011: Initial release

    authors:        Gavin Norman

    Command line arguments:

      -n, --nodes  the number of dht nodes to calculate the hash ranges for
      -b, --bits   the number of bits for the hashes (defaults to 32)

*******************************************************************************/

module src.main.dhthashrange;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.text.Arguments;

private import tango.io.Stdout;



/*******************************************************************************

    Main

*******************************************************************************/

void main ( char[][] cl_args )
{
    auto args = new Arguments(cl_args[0]);
    args("nodes").aliased('n').params(1).required.help("the number of dht nodes to calculate the hash ranges for");
    args("bits").aliased('b').params(1).restrict(["32", "64"]).defaults("32").help("the number of bits for the hashes (defaults to 32)");

    if ( args.parse(cl_args[1..$]) )
    {
        auto nodes = args.getInt!(uint)("nodes");
        auto bits = args.getInt!(uint)("bits");
        ulong max, range;

        switch ( bits )
        {
            case 32: max = uint.max;   break;
            case 64: max = ulong.max;  break;
            default: assert(false); break;
        }

        range = max / nodes;

        ulong start;
        uint i;

        for ( i = 0; i < nodes - 1; i++ )
        {
            printRange(bits, i, start, start + range);
            start = start + range + 1;
        }

        printRange(bits, i, start, max);
    }
    else
    {
        args.displayErrors();
        args.displayHelp();
    }
}



/*******************************************************************************

    Displays a hash range to the console.

    Params:
        bits = bits in a dht hash
        number = node number
        start = start hash
        end = end hash

*******************************************************************************/

private void printRange ( uint bits, uint number, ulong start, ulong end )
{
    switch ( bits )
    {
        case 32: Stdout.formatln("{,3}: {:X8} .. {:X8}", number, start, end);   break; 
        case 64: Stdout.formatln("{,3}: {:X16} .. {:X16}", number, start, end); break; 
        default: assert(false); break;
    }
}

