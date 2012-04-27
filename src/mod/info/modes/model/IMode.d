/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        March 2012: Initial release

    authors:        Hatem Oraby, Gavin Norman

    The module contains the IMode abstract class. The class is expected to be
    inherited by all mode display. The class contains the public functions that
    the src.mode.info.DhtInfo class calls in its event-loop.

*******************************************************************************/

module src.mod.info.modes.model.IMode;

/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.info.NodeInfo;

private import swarm.dht.DhtClient;

private import ocean.text.convert.Layout;


/*******************************************************************************

    The class contains the interfaces that all the mode displays should
    implement.

*******************************************************************************/

public abstract class IMode
{
    /***************************************************************************

        Refers to error back delegate that should recieve all the errors. The
        value of this variable (i.e. the delegate) is passed to this class's
        constructor during the class instantiation.

    ***************************************************************************/

    protected DhtClient.RequestNotification.Callback notifier;


    /***************************************************************************

        The dht client that is being used.

    ***************************************************************************/

    protected DhtClient dht;


    /***************************************************************************

        The dht nodes in the format of a NodeInfo array.

    ***************************************************************************/

    protected NodeInfo[] nodes;

    /***************************************************************************

        The id string that should be used to refer to this dht in printing
        information.

    ***************************************************************************/

    protected char[] dht_id;


    /***************************************************************************

        Holds the longes node name's length.

    ***************************************************************************/

    private int longest_node_name;


    /***************************************************************************

        A reference to an error callback function.

    ***************************************************************************/

    public alias void delegate (char[]) ErrorCallback;
    private ErrorCallback error_callback ;


    /***************************************************************************

        The constructor just assigns the parameter to the local class variables
        and fill up the local NodesInfo with the DhtClient nodes.

        Params:
            dht = The dht client that the mode will use.

            dht_id = The name of the DHT that this class is handling. The name
                is used in printin information.

            error_calback = The callback that the display-mode will call to
                pass to it the error messages that it has.

    ***************************************************************************/

    public this (DhtClient dht, char[] dht_id,
                 ErrorCallback error_callback)
    {
        this.dht = dht;
        this.error_callback = error_callback;

        foreach ( dht_node; dht.nodes )
        {
            auto node = NodeInfo(dht_node.address, dht_node.port,
                    dht_node.hash_range_queried, dht_node.min_hash,
                    dht_node.max_hash);

            this.nodes ~= node;

            if ( node.nameLength() > this.longest_node_name )
            {
                this.longest_node_name = node.nameLength();
            }
        }

        this.dht_id = dht_id;
    }


    /***************************************************************************

        This method is called by the event-loop. This method is expected to
        perform all the required steps to initiate an asynchronous call to the
        desiginated Dht (i.e. the implementing method should call
        DhtClient.assign(*required operation*).

        Note:
        On calling Dht.assign(Dht.requiredOperation(&cb, &errBack)) , the
        errBack should be the this.notifier (without "&" byref).
        The &cb shouldn't be an on-the-fly delegate (unnamed nested function)
        but should be instead a class method. On-the-fly nested functions
        tend to cause segmentation faults.

        The callbacks will be called when the control return to the event-loop
        but before the display method is called.

        Returns:
            The method should return true if it's operations depends on several
            consequent dependent calls, in that case returning true will signal
            to the event loop that this class instance still have more
            operations to perform and should be called again before it's ready
            to display.
            If the class has only the current waiting operation to be performed
            and nothing else is needed to be performed after that, then the
            method should return false.

            After each return (whether true or false), the event-loop will
            run the network event-loop (i.e perform the requested DHT operation
            on the network) and will check afterwards if the disaplay mode has
            previously signaled that it want to run again.
            If it detected that that it didn't then it will proceed to the
            display phase.
            If it detected that it did signal that it wanted to run agin, then
            it will run it again. However there is nothing to signal in which
            iteration is this, so in case of implementing a multi-operation
            dependent dht, then the instance itself has to keep track of its
            current status.

    ***************************************************************************/

	public abstract bool run ();


    /***************************************************************************

        This method is called after the run method and it's callbacks are
        called. This method should output to the stds the results in the
        required format.

        Params:
            longest_node_name = It's the size of the longest node name across
                all the dhts and not just this dht.

    ***************************************************************************/
	
	public abstract void display (size_t longest_node_name);


    /***************************************************************************

        The method returns the nodes that hasn't finished yet, if all finished
        then an empty list is returned.

        Return:
            The nodes that hasn't responded yet.

    ***************************************************************************/

 
    public NodeInfo[] whoDidntFinish ()
    {
        NodeInfo[] suspects;
        foreach (node; this.nodes)
        {
            if (!node.responded)
            {
                suspects ~= node;
                
            }
        }
        return suspects;
    }


    /***************************************************************************

        Returns the id of the DHT that this mode is handling.

        Return:
            The dht id.

    ***************************************************************************/

    public char[] getDhtId()
    {
        return this.dht_id;
    }


    /***************************************************************************

        Finds a node matching the provided address and port in the list of
        nodes.

        Params:
            address = address to match
            port = port to match

        Returns:
            pointer to matched NodeInfo struct in this.nodes, may be null if no
            match found

    ***************************************************************************/

    public NodeInfo* findNode ( char[] address, ushort port )
    {
        NodeInfo* found = null;

        foreach ( ref node; this.nodes )
        {
            if ( node.address == address && node.port == port )
            {
                found = &node;
                break;
            }
        }

        return found;
    }


    /***************************************************************************

        Returns the length of longest node name.

        Returns:
            Length of longest id.

    ***************************************************************************/

    public int getLongestNodeName()
    {
        return this.longest_node_name;
    }


    /***************************************************************************

        A DHT.assign callback. It counts the number of nodes that has responded,
        afterwards it passes the callback struct info to the global notifier
        that has been passed to this instance on it's creation.

        Params:
            info = The notification struct

    ***************************************************************************/

    private void local_notifier( DhtClient.RequestNotification info )
    {
        if ( info.type == info.type.Finished )
        {
            auto node = this.findNode(info.nodeitem.Address,
                                                info.nodeitem.Port);

            node.responded = true;;

            if (!info.succeeded )
            {
                char [] preappend = "";
                Layout!(char).print(preappend, "{}:{}:{} : ", this.dht_id,
                                    info.nodeitem.Address, info.nodeitem.Port);
                if (this.error_callback)
                {
                    error_callback(preappend ~ info.message());
                }
            }
        }
    }
}

