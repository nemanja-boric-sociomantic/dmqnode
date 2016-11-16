/*******************************************************************************

    The public channel access interface. The DiskOverflow.Channel subclass is
    instantiatable in the public.

    Copyright (c) 2015 sociomantic labs. All rights reserved

*******************************************************************************/

module dmqnode.storage.engine.OverflowChannel;

import dmqnode.storage.engine.DiskOverflow;
import dmqnode.storage.engine.overflow.ChannelMetadata;
import dmqnode.storage.engine.overflow.RecordHeader;

import ocean.transition;

package class OverflowChannel: DiskOverflowInfo
{
    /***************************************************************************

        The channel name.

    ***************************************************************************/

    private istring name;

    /***************************************************************************

        The host of the disk queue; queue access methods of this instance
        forward the calls to the host.

    ***************************************************************************/

    private DiskOverflow host;

    /***************************************************************************

        Pointer to the channel metadata maintained in this.host. The referenced
        object may be modified by the host itself without this instance doing
        anything, or by another instance of this class that refers to the same
        channel.

    ***************************************************************************/

    private ChannelMetadata* metadata;

    /**************************************************************************/

    invariant ( )
    {
        assert(this.metadata);
    }

    /***************************************************************************

        Constructor. Obtains a handle for channel_name, creating the channel if
        it doesn't exists.

        Params:
            host         = the host of the disk queue
            channel_name = channel name

    ***********************************************************************/

    package this ( DiskOverflow host, istring channel_name )
    {
        this.name     = channel_name;
        this.host     = host;
        this.metadata = host.getChannel(channel_name);
    }

    /***************************************************************************

        Pushes a record to this channel.

        Params:
            data    = record data

        Throws:
            FileException on file I/O error or data corruption.

    ***************************************************************************/

    public void push ( void[] data )
    {
        this.host.push(*this.metadata, data);
    }

    /***************************************************************************

        Pops a record to this channel.

        Calls get_buffer with the record length n; get_buffer is expected to
        return an array of length n. Populates that buffer with the record data.
        Does not call get_buffer if the queue was empty.

        Params:
            get_buffer = callback delegate to obtain the destination buffer for
                         the record data

        Returns:
            true if a record was popped or false if the queue was empty.

        Throws:
            FileException on file I/O error.

    ***************************************************************************/

    public bool pop ( void[] delegate ( size_t n ) get_buffer )
    {
        return this.host.pop(*this.metadata, get_buffer);
    }

    /***************************************************************************

        Resets the state of this channel to empty.

        If there are records in other channels, the record data of this channel
        remain untouched but are not referenced any more. If all other channels
        are empty or this is the only channel, the data and index file are
        truncated to zero size.

    ***************************************************************************/

    public void clear ( )
    {
        this.host.clearChannel(*this.metadata);
    }


    /***************************************************************************

        Renames this channel.

        Params:
            `new_name` = new channel name

    ***************************************************************************/

    public void rename ( istring new_name )
    {
        auto old_name = this.name;
        this.name = new_name;
        this.metadata = this.host.renameChannel(old_name, this.name);
    }

    /***************************************************************************

        Returns:
            the number of records in this channel.

    ***************************************************************************/

    public uint num_records ( )
    {
        return this.metadata.records;
    }

    /***************************************************************************

        Returns:
            the amount of payload bytes of all records in this channel.

    ***************************************************************************/

    public ulong num_bytes ( )
    {
        return this.metadata.bytes;
    }

    /***************************************************************************

        Returns:
            the total amount of bytes occupied by all records in this
            channel.

    ***************************************************************************/

    public ulong length ( )
    {
        return this.metadata.bytes + this.metadata.records * RecordHeader.sizeof;
    }
}
