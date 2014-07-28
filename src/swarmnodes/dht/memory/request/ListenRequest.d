/*******************************************************************************

    Listen request class.

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module swarmnodes.dht.memory.request.ListenRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.common.request.helper.LoopCeder;

private import swarm.core.common.request.helper.DisconnectDetector;

private import swarm.dht.DhtHash;

private import swarmnodes.dht.common.request.model.IChannelRequest;

private import swarmnodes.dht.common.storage.DhtStorageEngine;

private import ocean.core.Array : copy, pop; // TODO: copy not used?

private import ocean.io.select.client.FiberSelectEvent;

private import tango.util.log.Log;

debug private import ocean.util.log.Trace;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("swarmnodes.dht.common.request.ListenRequest");
}



/*******************************************************************************

    Listen request

*******************************************************************************/

public scope class ListenRequest : IChannelRequest, DhtStorageEngine.IListener
{
    /***************************************************************************

        Storage channel being read from. The reference is only set once the
        request begins processing.

    ***************************************************************************/

    private DhtStorageEngine storage_channel;


    /***************************************************************************

        Set to true when the handle___() method is waiting for the fiber select
        event to be triggered.

    ***************************************************************************/

    private bool waiting_for_trigger;


    /***************************************************************************

        Set to true when a buffer flush is requested.

    ***************************************************************************/

    private bool flush_trigger;


    /***************************************************************************

        Set to true when the request must end (in the case where the channel
        being listened to is deleted).

    ***************************************************************************/

    private bool finish_trigger;


    /***************************************************************************

        Maximum length of the internal list of record hashes which have been
        modified and thus need to be forwarded to the listening client. A
        maximum is given to avoid the (presumably extreme) situation where the
        hash buffer is growing indefinitely.

    ***************************************************************************/

    private const HashBufferMaxLength = (1024 / hash_t.sizeof) * 256; // 256 Kb of hashes


    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        IDhtRequestResources resources )
    {
        super(DhtConst.Command.E.Listen, reader, writer, resources);
    }


    /***************************************************************************

        IListener interface method. Called when a record in the listened channel
        has changed, the write buffer needs flushing, or the listener should
        finish.

        Params:
            code = trigger type
            key = key of put record
            value = put record value

    ***************************************************************************/

    public void trigger ( Code code, char[] key )
    {
        with ( Code ) switch ( code )
        {
            case DataReady:
                if ( (*this.resources.hash_buffer).length < HashBufferMaxLength )
                {
                    //This could lead to that the buffer containing the same key
                    //several times. Since the buffer is flushed often and
                    //checking the value before adding it could be a to heavy
                    //cost there's no need to do a check before adding the key.
                    (*this.resources.hash_buffer) ~= DhtHash.straightToHash(key);
                }
                else
                {
                    log.warn("Listen request on channel '{}', hash buffer reached maximum length -- record discarded",
                        *this.resources.channel_buffer);
                }
                break;

            case Flush:
                this.flush_trigger = true;
                break;

            case Finish:
                this.finish_trigger = true;
                break;
        }

        if ( this.waiting_for_trigger )
        {
            this.resources.event.trigger;
        }
    }


    /***************************************************************************

        Reads any data from the client which is required for the request. If the
        request is invalid in some way (the channel name is invalid, or the
        command is not supported) then the command can be simply not executed,
        and all client data has been read, leaving the read buffer in a clean
        state ready for the next request.

    ***************************************************************************/

    protected void readRequestData_ ( )
    {
    }


    /***************************************************************************

        Performs this request. (Fiber method.)

        First the storage channel is fetched and a status code is returned to
        the client -- Error if the channel does not exist, or Ok if the channel
        does exist. If the channel exists, this class is registered with it as a
        listener, which will cause the trigger() method to be called when data
        in the channel changes, when the listener should be flushed, or when the
        channel is deleted.

        The handler then enters a loop of waiting for a trigger then either
        flushing the write buffer or writing records to the client, depending on
        which trigger(s) occurred.

    ***************************************************************************/

    protected void handle___ ( )
    {
        this.storage_channel = this.resources.storage_channels.getCreate(
            *this.resources.channel_buffer);
        if ( this.storage_channel is null )
        {
            this.writer.write(DhtConst.Status.E.Error);
            return;
        }

        this.writer.write(DhtConst.Status.E.Ok);

        this.storage_channel.registerListener(this);
        scope ( exit )
        {
            this.storage_channel.unregisterListener(this);
        }

        scope disconnect_detector = new DisconnectDetector(
                this.writer.fileHandle, &this.on_disconnection);

        // Note: the Finish code is only received when the storage channel being
        // listened to is recycled.
        while ( !this.finish_trigger )
        {
            (*this.resources.hash_buffer).length = 0;

            if ( !this.waitForTrigger(disconnect_detector) )
            {
                return; // nothing more to do if we are disconnected
            }

            // Process all pending records
            hash_t hash;
            while ( (*this.resources.hash_buffer).pop(hash) )
            {
                DhtHash.HexDigest hex_digest;
                auto hash_str = DhtHash.toString(hash, hex_digest);

                // Get record from storage engine
                this.storage_channel.get(hash_str, *this.resources.value_buffer);

                this.writer.writeArray(hash_str);
                this.writer.writeArray(*this.resources.value_buffer);

                this.resources.node_info.handledRecord();

                this.resources.loop_ceder.handleCeding();
            }

            // Handle flushing
            if ( this.flush_trigger )
            {
                this.writer.flush();
                this.flush_trigger = false;
            }
        }

        // Write empty key and value, informing the client that the request has
        // finished
        this.writer.writeArray("");
        this.writer.writeArray("");
    }


    /***************************************************************************

        Waits for the select event to fire (see the trigger() method, above).

        Returns:
            False if a disconnection happened, true otherwise.

    ***************************************************************************/

    private bool waitForTrigger ( DisconnectDetector disconnect_detector )
    {
        this.waiting_for_trigger = true;
        scope ( exit ) this.waiting_for_trigger = false;

        // Listen for disconnections instead of "write" events while we are
        // waiting. When the wait is over, is either because we have more
        // records to send or we need to flush, both operations needing to
        // activate the "write" events again (or we disconnected, in which case
        // the cleanup also assumes the "write" event was active).
        this.writer.fiber.unregister();
        this.reader.fiber.epoll.register(disconnect_detector);
        scope (exit)
        {
            this.reader.fiber.epoll.unregister(disconnect_detector);
            // Avoid registering a dead fd.
            if ( !disconnect_detector.disconnected )
            {
                this.writer.fiber.register(this.writer);
            }
        }

        this.resources.event.wait;

        return !disconnect_detector.disconnected;
    }


    /***************************************************************************

        Action to trigger when a disconnection is detected.

    ***************************************************************************/

    private void on_disconnection ( )
    {
        if ( this.waiting_for_trigger )
        {
            this.resources.event.trigger;
        }
    }
}

