/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        March 2012: Initial release

    authors:        Gavin Norman, Hatem Oraby


    The display-mode displays minimal information about a given dht.
    The mode prints the following for the given dht:
        - The number of nodes running and the total number of connections
          coming to the all nodes of the dht.
        - If something is abnormal (e.g. failed nodes or long response time),
          then the method then outputs the error data in red color.

    When several dht files are passed and this mode is used, then this display-
    mode (coupled with -p flag or with linux watch) form an efficient tool
    that provides a summary on the overall states of all thr running DHTs.

*******************************************************************************/

module src.mod.info.modes.MinimalMode;

/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.info.modes.model.IMode;

private import swarm.dht.DhtClient;

private import ocean.io.Stdout;

private import tango.time.StopWatch;


class MinimalMode : IMode
{
    /***************************************************************************

        The stopwatch is used to monitor how long did a given dht request take
        to respond back.

    ***************************************************************************/

    private StopWatch sw;


    /***************************************************************************

        Used for display formatting purposes.

    ***************************************************************************/

    private char[] padding;


    /***************************************************************************

        Counts the number of nodes that has responded.

    ***************************************************************************/

    private uint responded;


    /***************************************************************************

        Holds the total number of connections that all the nodes have.

    ***************************************************************************/

    private uint connections;


    /***************************************************************************

        Holds the longest time that a node has taken to respond.

    ***************************************************************************/

    private ulong end_time;


    /***************************************************************************

        TODO: comment

    ***************************************************************************/

    public this (DhtClient dht, char[] dht_id,
                IMode.ErrorCallback error_callback)
    {
            super(dht, dht_id, error_callback);
    }


    /***************************************************************************

        TODO: comment

    ***************************************************************************/

    public bool run ()
    {
        foreach (ref node; this.nodes)
        {
            node.responded = false;
        }

        this.responded = 0;
        this.connections = 0;

        sw.start();
        this.dht.assign(this.dht.getNumConnections( &this.getNumConnsCb,
                                                    &this.local_notifier));

        return false;
    }


    /***************************************************************************

        TODO: comment

    ***************************************************************************/

    public void display ( size_t longest_dht_name )
    {
        padding.length =
            longest_dht_name - this.getDhtId().length;
        padding[] = ' ';

        Stdout.magenta.bold;
        Stdout.format("{}{}: ", padding, this.dht_id);
        Stdout.default_colour.bold(false);
        if ( this.responded == this.nodes.length )
        {
            Stdout.format("{} nodes", this.responded);
        }
        else
        {
            Stdout.red_bg;
            Stdout.format("{} / {} nodes", this.responded,
                            this.nodes.length);
            Stdout.default_bg;
        }
        Stdout.format(", ");
        Stdout.format("{} connections", this.connections);

        Stdout.format(", ");
        auto ms = (this.end_time) / 1_000;
        if ( ms > 500 )
        {
            Stdout.red_bg;
            Stdout.format("{}ms", ms);
            Stdout.default_bg;
        }
        else
        {
            Stdout.format("{}ms", ms);
        }
        Stdout.newline.flush;
    }


    /***************************************************************************

        OVerrides the parent function and returns the dht id length.

        Returns:
            DHT id length.

    ***************************************************************************/

    public int getLongestNodeName()
    {
        return super.dht_id.length;
    }


    /***************************************************************************

        TODO: comment

    ***************************************************************************/

    private void getNumConnsCb ( DhtClient.RequestContext,
                                char[] address, ushort port, size_t conns )
    {
        this.end_time = sw.microsec;

        this.responded++;
        this.connections += conns;
    }
}

