/*******************************************************************************

    copyright: Copyright (c) 2016 sociomantic labs. All rights reserved

    Consume request implementation.

*******************************************************************************/

module dmqnode.request.neo.Consume;

import dmqnode.connection.neo.SharedResources;
import swarm.core.neo.node.ConnectionHandler;
import dmqnode.storage.model.StorageEngine;
import ocean.core.TypeConvert : downcast;

/*******************************************************************************

    The request handler for the table of handlers. When called, runs in a fiber
    that can be controlled via `connection`.

    Params:
        shared_resources = an opaque object containing resources owned by the
            node which are required by the request
        connection  = performs connection socket I/O and manages the fiber
        cmdver      = the version number of the Consume command as specified by
                      the client
        msg_payload = the payload of the first message of this request

*******************************************************************************/

void handle (
    Object shared_resources,
    ConnectionHandler.RequestOnConn connection,
    ConnectionHandler.Command.Version cmdver,
    void[] msg_payload
)
{
    auto dmq_shared_resources = downcast!(SharedResources)(shared_resources);
    assert(dmq_shared_resources);

    scope c = new Consume(connection, msg_payload, dmq_shared_resources);
}

/******************************************************************************/

public scope class Consume : StorageEngine.IConsumer
{
    import swarm.dmq.DmqConst;
    import swarm.dmq.neo.protocol.Consume;
    import swarm.core.neo.util.StateMachine;
    import dmqnode.storage.model.StorageChannels;
    import swarm.core.neo.node.RequestOnConn;
    import swarm.core.neo.connection.RequestOnConnBase;

    import ocean.transition;
    import ocean.core.TypeConvert : castFrom;

    mixin(genStateMachine([
        "Sending",
        "Suspended",
        "WaitingForData"
    ]));

    /***************************************************************************

        Codes used when resuming the fiber to interrupt waiting for I/O.

    ***************************************************************************/

    private enum NodeFiberResumeCode: uint
    {
        Pushed = 1,
        ChannelRemoved = 2
    }

    /***************************************************************************

        Thrown to cancel the request if the channel was removed.

    ***************************************************************************/

    static class ChannelRemovedException: Exception
    {
        this () {super("Channel removed");}
    }

    /***************************************************************************

        Request-on-conn, to get the event dispatcher and control the fiber.

    ***************************************************************************/

    private RequestOnConn connection;

    /***************************************************************************

        Request-on-conn event dispatcher, to send and receive messages.

    ***************************************************************************/

    private RequestOnConn.EventDispatcher ed;

    /***************************************************************************

        Message parser

    ***************************************************************************/

    private RequestOnConn.EventDispatcher.MessageParser parser;

    /***************************************************************************

        Global parameters and shared resources.

    ***************************************************************************/

    private SharedResources shared_resources;

    /***************************************************************************

        Storage engine of the channel consuming from.

    ***************************************************************************/

    private StorageEngine storage_engine;

    /***************************************************************************

        If true, trigger() resumes the fiber if a record was pushed.

    ***************************************************************************/

    private bool resume_fiber_on_push;

    /***************************************************************************

        Constructor; executes the request.

        Params:
            connection  = the request-on-conn managing this request
            msg_payload = the payload of the first message for this request
            shared_resources = global shared resources

    ***************************************************************************/

    this ( RequestOnConn connection, void[] msg_payload,
           SharedResources shared_resources )
    in
    {
        assert(shared_resources);
    }
    body
    {
        this.connection = connection;
        this.ed = connection.event_dispatcher;
        this.parser = this.ed.message_parser;

        char[] channel_name;
        StartState start_state;
        this.parser.parseBody(msg_payload, channel_name, start_state);

        State state;
        switch ( start_state )
        {
            case StartState.Running:
                state = state.Sending;
                break;
            case StartState.Suspended:
                state = state.Suspended;
                break;
            default:
                this.ed.shutdownWithProtocolError("invalid start state");
        }

        this.shared_resources = shared_resources;
        this.storage_engine =
            this.shared_resources.storage_channels.getCreate(channel_name);
        if ( !this.storage_engine )
        {
            this.ed.sendT(DmqConst.Status.E.Error);
            return;
        }

        this.ed.sendT(DmqConst.Status.E.Ok);

        try
        {
            this.storage_engine.registerConsumer(this);
            this.run(state);
            return;
        }
        catch (ChannelRemovedException e)
        {
            // Call sendChannelRemoved() below outside the catch clause to avoid
            // a fiber context switch inside the runtime exception handler.
        }
        finally
        {
            this.storage_engine.unregisterConsumer(this);
        }

        this.sendChannelRemoved();
    }

    /***************************************************************************

        Sending state: Pop records from the queue and send them to the client.

    ***************************************************************************/

    private State stateSending ( )
    {
        scope resources = this.shared_resources.new RequestResources;
        char[]* value = castFrom!(void[]*).to!(char[]*)(resources.getValueBuffer());

        for (this.storage_engine.pop(*value); value.length;
            this.storage_engine.pop(*value))
        {
            bool received_msg;
            MessageType msg_type;

            this.ed.sendReceiveT(
                ( in void[] msg )
                {
                    this.parser.parseBody(msg, msg_type);
                    received_msg = true;
                },
                MessageType.Record, *value
            );

            if (received_msg)
            {
                // sendReceiveT() was interrupted while sending so send
                // again. No need to receive as well; the client ensures
                // that the next message can't be sent until it has
                // received the ACK.
                scope (success)
                {
                    this.ed.sendT(MessageType.Record, *value);
                    this.ed.sendT(MessageType.Ack);
                }

                switch ( msg_type )
                {
                    case msg_type.Suspend:
                        return State.Suspended;

                    case msg_type.Stop:
                        return State.Exit;

                    case msg_type.Resume:
                        // Ignore, already running
                        break;

                    default:
                        throw this.ed.shutdownWithProtocolError(
                            "Consume: invalid message from client");
                }
            }
        }

        return State.WaitingForData;
    }

    /***************************************************************************

        Suspended state: Wait until the client resumes or stops the request.

    ***************************************************************************/

    private State stateSuspended ( )
    {
        // No need to receive as well; the client ensures that the next
        // message can't be sent until it has received the ACK.
        scope (success)
            this.ed.sendT(MessageType.Ack);

        switch (this.ed.receiveValue!(MessageType)())
        {
            case MessageType.Suspend:
                // It's not expected to receive Suspend messages while
                // already suspended, but it does no harm.
                return State.Suspended;

            case MessageType.Resume:
                return State.Sending;

            case MessageType.Stop:
                return State.Exit;

            default:
                throw this.ed.shutdownWithProtocolError(
                    "Consume: invalid message from client");
        }
    }

    /***************************************************************************

        WaitingForData state: Wait until either a record is pushed into the
        queue or the client suspends or stops the request.

    ***************************************************************************/

    private State stateWaitingForData ( )
    {
        MessageType msg_type;
        int resume_code;

        this.resume_fiber_on_push = true;
        try
        {
            resume_code = this.ed.receive(
                (in void[] msg) {this.parser.parseBody(msg, msg_type);}
            );
        }
        finally
        {
            this.resume_fiber_on_push = false;
        }

        if (resume_code > 0) // positive code => user code => must be Pushed
        {
            assert(resume_code == NodeFiberResumeCode.Pushed,
                   "Consume: unexpected fiber resume message");
            return State.Sending;
        }

        // We called unregisterConsumer() so the fiber can only be resumed by an
        // I/O event.
        this.ed.sendT(MessageType.Ack);

        switch (msg_type)
        {
            case msg_type.Suspend:
                return State.Suspended;

            case msg_type.Stop:
                return State.Exit;

            case msg_type.Resume:
                // Already running: This resume message is pointless but
                // acceptable. Switch to Sending state, which will most
                // likely pop nothing and switch to WaitingForData state
                // again.
                return State.Sending;

            default:
                throw this.ed.shutdownWithProtocolError(
                    "Consume: invalid message from client");
        }
    }


    /***************************************************************************

        Sends a "channel removed" message to the client, ignoring messages
        received from the client. The fiber should not be resumed by consumer
        events.

    ***************************************************************************/

    private void sendChannelRemoved ( )
    {
        bool send_interrupted;

        do
        {
            this.ed.sendReceiveT(
                (in void[] msg) {send_interrupted = true;},
                MessageType.ChannelRemoved
            );
        }
        while (send_interrupted);
    }

    /***************************************************************************

        StorageEngine.IConsumer method, called when a record is pushed (or
        another event happened, which we currently don't handle).

        Params:
            code = event code

    ***************************************************************************/

    override public void trigger ( Code code )
    {
        switch ( code )
        {
            case code.DataReady:
                if (this.resume_fiber_on_push)
                    this.connection.resumeFiber(NodeFiberResumeCode.Pushed);
                break;

            case code.Flush:
                break; // Nothing to do

            case code.Finish:
                // This happens only at most once a month so it is safe to use a
                // new Exception.
                this.connection.resumeFiber(new ChannelRemovedException);
                break;

            default:
                assert(false);
        }
    }
}
