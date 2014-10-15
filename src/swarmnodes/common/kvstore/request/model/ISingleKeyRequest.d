/*******************************************************************************

    Base class for a request over a single key in a specific channel.

    copyright: Copyright (c) 2014 sociomantic labs. All rights reserved

*******************************************************************************/

module swarmnodes.common.kvstore.request.model.ISingleKeyRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.common.kvstore.request.model.IChannelRequest;

private import swarm.core.Const;

private import ocean.text.convert.Layout;



/*******************************************************************************

    Single key request base class

*******************************************************************************/

public abstract scope class ISingleKeyRequest : IChannelRequest
{
    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( DhtConst.Command.E cmd, FiberSelectReader reader,
        FiberSelectWriter writer, IKVRequestResources resources )
    {
        super(cmd, reader, writer, resources);
    }


    /***************************************************************************

        Formats a description of this command into the provided buffer. The
        default implementation formats the name of the command, the channel, and
        the key on which it operates. Derived request classes may override and
        add more detailed information.

        Params:
            dst = buffer to format description into

        Returns:
            description of command (slice of dst)

    ***************************************************************************/

    override public char[] description ( ref char[] dst )
    {
        super.description(dst);

        auto key = *this.resources.key_buffer;
        Layout!(char).print(dst, " for key 0x{}", key.length ? key : "?");
        return dst;
    }


    /***************************************************************************

        Reads any data from the client which is required for the request. If the
        request is invalid in some way (the channel name is invalid, or the
        command is not supported) then the command can be simply not executed,
        and all client data has been read, leaving the read buffer in a clean
        state ready for the next request.

    ***************************************************************************/

    final protected void readRequestData_ ( )
    {
        super.reader.readArray(*this.resources.key_buffer);

        this.readRequestData__();
    }

    protected void readRequestData__ ( )
    {
    }
}

