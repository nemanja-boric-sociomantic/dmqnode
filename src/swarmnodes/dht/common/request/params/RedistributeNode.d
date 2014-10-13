/*******************************************************************************

    Copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

    Struct used to deserialize a list of node address / hash range tuples while
    handling Redistribute requests.

*******************************************************************************/

module swarmnodes.dht.common.request.params.RedistributeNode;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.Const : NodeItem;

private import swarm.dht.DhtConst : HashRange;



public struct RedistributeNode
{
    /***************************************************************************

        IP address / port of node

    ***************************************************************************/

    public NodeItem node;


    /***************************************************************************

        Hash responsibility range of node

    ***************************************************************************/

    public HashRange range;
}


