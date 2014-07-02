/*******************************************************************************

    Copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

    Class to manage the range of hashes handled by a dht node, including the
    ability to modify the range and update the config file with the new range.

    The hash range takes one of two forms:
        1. The standard form. Min hash <= max hash.
        2. Empty. Min hash and max hash both have magic values (see
           ocean.math.Range), allowing this state to be distinguished.

    The empty state is supported to allow new nodes to be started up with no
    current hash responsibility, awaiting an external command to tell them which
    range they should support. It could also be used to effectively delete a
    node by setting its hash range to empty.

*******************************************************************************/

module swarmnodes.dht.common.node.DhtHashRange;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.dht.common.app.config.HashRangeConfig;

private import swarm.dht.DhtConst : HashRange;

private import ocean.core.Exception : enforce;



public class DhtHashRange
{
    /***************************************************************************

        Min & max hash values.

    ***************************************************************************/

    private HashRange range_;


    /***************************************************************************

        Config file updater.

    ***************************************************************************/

    private const HashRangeConfig config_file;


    /***************************************************************************

        Constructor. Sets the range as specified.

        Params:
            min = min hash
            max = max hash
            config_file = config file updater

        Throws:
            if the range specified by min & max is invalid

    ***************************************************************************/

    public this ( hash_t min, hash_t max, HashRangeConfig config_file )
    in
    {
        assert(config_file);
    }
    body
    {
        this.config_file = config_file;

        enforce(HashRange.isValid(min, max), "Invalid hash range");
        this.range_ = HashRange(min, max);
    }


    /***************************************************************************

        Returns:
            hash range

    ***************************************************************************/

    public HashRange range ( )
    {
        return this.range_;
    }


    /***************************************************************************

        Returns:
            true if the hash range is empty

    ***************************************************************************/

    public bool is_empty ( )
    {
        return this.range.is_empty;
    }


    /***************************************************************************

        Sets the hash range and updates the config file(s).

        Params:
            min = min hash
            max = max hash

        Throws:
            if the specified range is invalid

    ***************************************************************************/

    public void set ( hash_t min, hash_t max )
    {
        this.config_file.set(min, max);

        enforce(HashRange.isValid(min, max), "Invalid hash range");
        this.range_ = HashRange(min, max);
    }


    /***************************************************************************

        Sets the hash range to empty and updates the config file(s).

    ***************************************************************************/

    public void clear ( )
    {
        this.config_file.clear();
        this.range_ = this.range_.init;
    }
}

