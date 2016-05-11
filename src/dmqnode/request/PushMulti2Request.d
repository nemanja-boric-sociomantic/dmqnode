/*******************************************************************************

    Push request for multiple channels (new version)

    Explanation of processing logic:

    The status code which is sent to the client and the behaviour of this
    request handler are determined as follows:
        * If the received value is empty, then the EmptyValue code is
          returned to the client and nothing is pushed.
        * If pushing the received record into the specified number of
          channels would exceed the global size limit, then the OutOfMemory
          code is returned to the client and nothing is pushed.
        * If any of the received channel names is invalid, then the
          BadChannelName code is returned and nothing is pushed.
        * If any of the specified channels does not exist or cannot be
          created, then the Error code is returned to the client and nothing
          is pushed.
        * Otherwise, the Ok code is returned to the client, the record is
          pushed to each channel, and the names of any channels to which the
          record could not be pushed are sent to the client, as described
          below.

    The pushing behaviour (occurring when the Ok status is returned to the
    client) is as follows:
        * For each channel specified, if the received record fits in the
          space available, then it is pushed.
        * If the received record does not fit in the space available in a
          channel, then the channel's name is sent to the client.
        * When all of the specified channels have been handled, an end-of-
          list terminator (an empty string) is sent to the client.

    Thus, in the case when the dmqnode is able to push the received record
    into all of the specified channels, the client will receive the Ok
    status followed by an empty string (indicating an empty list of failed
    channels).

    copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

*******************************************************************************/

module dmqnode.request.PushMulti2Request;

/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.Const;

private import dmqnode.request.model.IDmqRequestResources;

private import Protocol = dmqproto.node.request.PushMulti2;

/*******************************************************************************

    PushMulti request

*******************************************************************************/

public scope class PushMulti2Request : Protocol.PushMulti2
{
    /***************************************************************************

        Shared resource acquirer

    ***************************************************************************/

    private const IDmqRequestResources resources;

    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
            IDmqRequestResources resources )
    {
        super(reader, writer, resources);
        this.resources = resources;
    }
}
