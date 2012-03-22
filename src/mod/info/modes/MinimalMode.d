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


private import tango.time.StopWatch;


private import ocean.io.Stdout;

private import swarm.dht.DhtClient;


private import src.mod.info.modes.model.IMode;





class MinimalMode : IMode
{

    /***************************************************************************

    The stopwatch is used to monitor how long did a given dht request take
    to respond back.

    ***************************************************************************/

    public StopWatch sw;

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

    Hold the longest time that a node has taken to respond.

    ***************************************************************************/

    private ulong end_time;


    public this (DhtWrapper wrapper,
                DhtClient.RequestNotification.Callback notifier)
    {
            super(wrapper, notifier);
            sw = StopWatch();
    }


    public bool run ()
    {
        sw.start();
        this.responded = 0;
        this.connections = 0;

        this.wrapper.dht.assign(this.wrapper.dht.getNumConnections(
                                &this.getNumConnsCb, this.notifier));

        return false;
    }


    private void getNumConnsCb ( DhtClient.RequestContext,
                            char[], ushort, size_t conns )
    {
        this.end_time = sw.microsec;

        this.responded++;
        this.connections += conns;
    }




    public void display ( size_t longest_dht_name )
    {
        padding.length =
            longest_dht_name - this.wrapper.dht_id.length;
        padding[] = ' ';

        Stdout.magenta.bold;
        Stdout.format("{}{}: ", padding, this.wrapper.dht_id);
        Stdout.default_colour.bold(false);
        if ( this.responded == this.wrapper.dht.nodes.length )
        {
            Stdout.format("{} nodes", this.responded);
        }
        else
        {
            Stdout.red_bg;
            Stdout.format("{} / {} nodes", this.responded,
                            this.wrapper.dht.nodes.length);
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

}


