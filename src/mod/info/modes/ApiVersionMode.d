/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        March 2012: Initial release

    authors:        Gavin Norman, Hatem Oraby


    The ApiVersionMode display-mode prints the api version for each node in a
    given DHT. A message will be printed if a node version mismatch was found.

*******************************************************************************/



module src.mod.info.modes.ApiVersionMode;


private import tango.core.Array : contains;


private import ocean.io.Stdout;

private import ocean.text.convert.Layout;

private import ocean.core.Array : appendCopy;


private import swarm.dht.DhtClient;


private import src.mod.info.modes.model.IMode;




class ApiVersionMode : IMode
{
    /***************************************************************************

    Acts as a buffer for all the text the will be printed.

    ***************************************************************************/

    private char[] final_string;

    /***************************************************************************

    Holds the API version of each node.

    ***************************************************************************/

    private char[][] nodes_versions;

    public this (DhtWrapper wrapper,
              DhtClient.RequestNotification.Callback notifier)
    {
            super(wrapper, notifier);
    }


    public bool run ()
    {
        // Query all nodes for their active connections
        this.wrapper.dht.assign(this.wrapper.dht.getVersion(
                &this.callback, this.notifier));
        return false;
    }



    void callback ( DhtClient.RequestContext context, char[] address,
                    ushort port, char[] api_version )
    {
        if ( api_version.length)
        {
            Layout!(char).print(this.final_string, "  {}:{} API: {}",
                                            address, port, api_version );
            if (this.nodes_versions.length &&
                !this.nodes_versions.contains(api_version))
                    Layout!(char).print(this.final_string, "\t NODE MISMATCH");
            Layout!(char).print(this.final_string, "\n");

            this.nodes_versions.appendCopy(api_version);
        }

    }

    /***************************************************************************

        Display the output.

    ***************************************************************************/

    public void display (size_t longest_node_name )
    {
        Stdout.formatln("\nApi version:");
        Stdout.formatln("------------------------------------------------------------------------------");
        Stdout.formatln(this.final_string);
        this.final_string.length = 0;
    }

}

