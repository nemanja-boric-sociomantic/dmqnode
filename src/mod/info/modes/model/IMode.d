/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        March 2012: Initial release

    authors:        Hatem Oraby, Gavin Norman


    The module contains two classes:

    IMode abstract class:
        The class is expected to be inherited by all mode display. The class
        contains the public functions that the src.mode.info.DhtInfo class
        calls in its event-loop.

    DhtWrapper struct:
        The struct is used by the IMode class. The struct encapsulates the
        attributes and functions that are commonly required by all the mode
        displays to refer to a single dht.

*******************************************************************************/

module src.mod.info.modes.model.IMode;


private import src.mod.info.NodeInfo;



private import swarm.dht.DhtClient;



/*******************************************************************************

    The class contains the interfaces that all the mode displays should
    implement.

*******************************************************************************/


public abstract class IMode
{

    /***************************************************************************

    Refers to the DHT and it's nodes that the display mode should handle.

    ***************************************************************************/

    protected DhtWrapper wrapper;


    /***************************************************************************

    Refers to error back delegate that should recieve all the errors. The value
    of this variable (i.e. the delegate) is passed to this class's constructor
    during the class instantiation.

    ***************************************************************************/

    protected DhtClient.RequestNotification.Callback notifier;


    public this (DhtWrapper wrapper,
              DhtClient.RequestNotification.Callback notifier)
    {
            this.wrapper = wrapper;
            this.notifier = notifier;
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
          bool repeat:
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
            longest_node_name:
                It's the size of the longest node name across
                all the dhts and not just this dht.
    ***************************************************************************/

	
	public abstract void display (size_t longest_node_name);
}




/***************************************************************************

    The wrapper encapsulates the most common dht-client data that
    all the IModes child classes use.

***************************************************************************/

public struct DhtWrapper
{
   /***************************************************************************

    The dht client that is being used.

    ***************************************************************************/

   public DhtClient dht;

   /***************************************************************************

    The dht nodes in the format of a NodeInfo array.

    ***************************************************************************/


   public NodeInfo[] nodes;

   /***************************************************************************

    The id string that should be used to refer to this dht in printing
    information.

    ***************************************************************************/

   public char[] dht_id;


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
}






