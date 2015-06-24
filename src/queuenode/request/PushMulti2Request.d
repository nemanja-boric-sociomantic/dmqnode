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

    Thus, in the case when the queuenode is able to push the received record
    into all of the specified channels, the client will receive the Ok
    status followed by an empty string (indicating an empty list of failed
    channels).

    copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

*******************************************************************************/

module queuenode.request.PushMulti2Request;

/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.Const;

private import queuenode.request.model.IQueueRequestResources;

private import Protocol = dmqproto.node.request.PushMulti2;

/*******************************************************************************

    PushMulti request

*******************************************************************************/

public scope class PushMulti2Request : Protocol.PushMulti2
{
    /***************************************************************************

        Shared resource acquirer

    ***************************************************************************/

    private const IQueueRequestResources resources;

    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
            IQueueRequestResources resources )
    {
        super(reader, writer, resources);
        this.resources = resources;
    }

    /***************************************************************************

        Ensure that requested channels exit / can be created and can be written
        to.

        Params:
            channel_name = list of channel names to checl

        Returns:
            "true" if all requested channels are available
            "false" otherwise

    ***************************************************************************/

    override protected bool prepareChannels ( char[][] channel_names )
    {
        foreach (channel; channel_names)
        {
            if (!this.resources.storage_channels.getCreate(channel))
                return false;
        }

        return true;
    }

    /***************************************************************************

        Ensure there is OK to store value of specific size according to global
        channel limits

        Params:
            value        = value to write

    ***************************************************************************/

    override protected bool canStoreValue ( size_t value_size )
    {
        return this.resources.storage_channels.sizeLimitOk(value_size);
    }

    /***************************************************************************

        PushMulti the value to the channel.

        Params:
            channel_name = name of channel to be writter to
            value        = value to write

        Returns:
            "true" if writing the value was possible
            "false" if there wasn't enough space

    ***************************************************************************/

    override protected bool pushValue ( char[] channel_name, void[] value )
    {
        auto channel = this.resources.storage_channels.getCreate(channel_name);
        assert (channel); // must be already verified in this.prepareChannels

        bool limit_ok = this.resources.storage_channels.sizeLimitOk(
            channel_name, value.length);

        if (limit_ok && channel.willFit(cast(char[]) value))
        {
            channel.push(cast(char[]) value);
            return true;
        }
        else
        {
            return false;
        }
    }
}
