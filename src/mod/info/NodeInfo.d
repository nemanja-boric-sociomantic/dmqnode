/*******************************************************************************

    Queried info about a dht node

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        April 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.info.NodeInfo;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.core.Array;

private import Integer = tango.text.convert.Integer;

private import tango.text.convert.Layout;

debug private import tango.util.log.Trace;



/*******************************************************************************

    Size info for a single node in a dht

*******************************************************************************/

public struct NodeInfo
{
    /***************************************************************************
    
        Node address & port
    
    ***************************************************************************/
    
    public char[] address;
    public ushort port;
    
    
    /***************************************************************************
    
        Node hash range
    
    ***************************************************************************/
    
    public bool range_queried;
    public hash_t min_hash;
    public hash_t max_hash;
    
    
    /***************************************************************************
    
        Number of connections handled by node
    
    ***************************************************************************/
    
    public size_t connections;
    
    
    /***************************************************************************
    
        Size info for a single channel in a dht node
    
    ***************************************************************************/
    
    public struct ChannelInfo
    {
        public char[] name;
        public ulong records;
        public ulong bytes;
    }
    
    
    /***************************************************************************
    
        Array of info on channels in a dht node
    
    ***************************************************************************/
    
    public ChannelInfo[] channels;
    
    
    /***************************************************************************
    
        Sets the size for a channel.
    
        Params:
            channel = channel name
            records = number of records in channel
            bytes = number of bytes in channel
    
    ***************************************************************************/
    
    public void setChannelSize ( char[] channel, ulong records, ulong bytes )
    {
        foreach ( ref ch; this.channels )
        {
            if ( ch.name == channel )
            {
                ch.records = records;
                ch.bytes = bytes;
                return;
            }
        }
    
        this.channels ~= ChannelInfo("", records, bytes);
        this.channels[$-1].name.copy(channel);
    }
    
    
    /***************************************************************************
    
        Gets the size for a channel into the provided output variables.
    
        Params:
            channel = channel name
            records = receives the number of records in channel
            bytes = receives the number of bytes in channel
            node_queried = receives the boolean telling whether the node
                responded to the query to get the size of this channel
    
    ***************************************************************************/
    
    public void getChannelSize ( char[] channel, out ulong records, out ulong bytes, out bool node_queried )
    {
        foreach ( ch; this.channels )
        {
            if ( ch.name == channel )
            {
                records += ch.records;
                bytes += ch.bytes;
                node_queried = true;
                return;
            }
        }
    }
    
    
    /***************************************************************************
    
        Formats the provided string with the name of this node.
    
        Params:
            name = string to receive node name
    
    ***************************************************************************/
    
    public void name ( ref char[] name )
    {
        typeof(*this).formatName(this.address, this.port, name);
    }
    
    
    /***************************************************************************
    
        Returns:
            the number of characters required by the node's name
    
    ***************************************************************************/

    public size_t nameLength ( )
    {
        return this.address.length + 1 + Integer.toString(port).length;
    }
    
    
    /***************************************************************************
    
        Formats the provided string with the hash range of this node.

        Params:
            name = string to receive node name

    ***************************************************************************/

    public void range ( ref char[] buf )
    {
        buf.length = 0;
    
        size_t layoutSink ( char[] str )
        {
            buf.append(str);
            return str.length;
        }
        
        Layout!(char).instance().convert(&layoutSink, "0x{:x8} .. 0x{:x8}", this.min_hash, this.max_hash);
    }

    
    /***************************************************************************
    
        Returns:
            the number of characters required by the node's hash range
    
    ***************************************************************************/

    public size_t rangeLength ( )
    {
        return 24;
    }


    /***************************************************************************
    
        Formats the provided string with the name of the specified node.
    
        Params:
            address = node ip address
            port = node port
            name = string to receive node name
    
    ***************************************************************************/
    
    static public void formatName ( char[] address, ushort port, ref char[] name )
    {
        name.concat(address, ":", Integer.toString(port));
    }
}

