/*******************************************************************************

    DHT node copy 

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        October 2010: Initial release

    authors:        Lars Kirchhoff

    --

    Copies the data of all channels from a source dht node cluster to the 
    destination dht node cluster.

    TODO: 
    1. Update to be based on DhtTool base class.
    2. handle compression properly
    3. handle get and put method properly
    4. use streaming instead of single puts 

 ******************************************************************************/

module mod.copy.DhtCopy;



/*******************************************************************************

    Imports

 ******************************************************************************/

private import core.dht.DestinationQueue;

private import  ocean.core.Array;

private import  ocean.text.Arguments;

private import  swarm.dht2.DhtClient,
                swarm.dht2.DhtHash,
                swarm.dht2.DhtConst;

private import  swarm.dht2.client.connection.ErrorInfo;

private import  tango.core.Array;

private import  Integer = tango.text.convert.Integer;

private import  tango.io.Stdout;

private import  tango.math.Math;

private import  tango.time.StopWatch;



/*******************************************************************************

    DhtCopy - starts the copy worker process

 ******************************************************************************/

struct DhtCopy
{
    public static bool run ( Arguments args )
    {
        scope worker = new DhtCopyWorker();

        if (args.getString("source").length != 0 && 
            args.getString("destination").length != 0 )
        {
            if (args.getString("channel").length != 0)
            {
                worker.setChannel(args.getString("channel"));
            }

            if (args.getBool("compression"))
            {
                worker.setCompression(true);
            }

            worker.dhtcopy(args.getString("source"), args.getString("destination"), args.getInt!(hash_t)("start"), args.getInt!(hash_t)("end"));

            return true;
        }

        if (args.getInt!(uint)("range") != 0)
        {
            worker.range(args.getInt!(uint)("range"));
            return true;
        }

        return false;
    }
}



class DhtCopyWorker
{
    /***************************************************************************

        Number of Connections to each DHT node

     **************************************************************************/

    private     const uint          SRC_CONNECTIONS = 1;
    private     const uint          DST_CONNECTIONS = 1;

    /***************************************************************************

        Dht client for the source nodes

     **************************************************************************/

    private     DhtClient           src;

    /***************************************************************************

        Dht client for the destination nodes

     **************************************************************************/

    private     DhtClient           dst;

    /***************************************************************************

        Record queue the destination dht nodes (to put in batches)
    
     **************************************************************************/
    
    private     DestinationQueue    dst_queue;

    /***************************************************************************

        Names of source channels 

     **************************************************************************/

    private     char[][]            src_channels;

    /***************************************************************************

        Number of items copied per channel

     **************************************************************************/

    private     uint[char[]]        channel_count;
    
    /***************************************************************************

        Internal record counter

     **************************************************************************/

    private     uint                records_count;

    /***************************************************************************

        Total number of bytes copied

     **************************************************************************/

    private     uint                records_bytes;

    /***************************************************************************

        Number of channel items to copy

     **************************************************************************/

    private     uint                channel_records;

    /***************************************************************************

        Number of progress steps shown while doing the test

     **************************************************************************/

    private     uint                progress_steps          = 50;

    /***************************************************************************

        Number that tells when to show the next progress step based on the 
        number of iteration and the number of progress steps.

     **************************************************************************/

    private     uint                progress_               = 1;

    /***************************************************************************

        StopWatch

     **************************************************************************/

    private     StopWatch           sw;

    /***************************************************************************

        Constructor

     **************************************************************************/

    public this () {}

    /***************************************************************************

        Set channel to the channel name provided

        Params:
            channel = channel name

        Returns:
            void

     **************************************************************************/

    public void setChannel ( char[] channel )
    {
        this.src_channels.length = 0;

        if (channel.length != 0)
        {
            this.src_channels ~= channel;
        }
    }

    /***************************************************************************

        Set if compression should be used for the destination 

        Params:
            compression = compression enabled 

        Returns:
            void

     **************************************************************************/

    public void setCompression ( bool compression = false )
    {
        this.dst_queue.setCompression(compression);
    }

    /***************************************************************************

        Copy the data from source dht node cluster to destination dht 
        node cluster

        Params:
            src_file = name of the source dht node cluster configuration
            dht_file = name of the destination dht node cluster configuration

        Returns:
            void

     **************************************************************************/

    public void dhtcopy ( char[] src_file, char[] dst_file, hash_t start, hash_t end )
    {
        this.initDhtClients(src_file, dst_file);

        this.initChannels();

        this.dst_queue.setRange(start, end);

        debug Stdout.formatln("Channels: {}\nCompression: {}\nRange: 0x{:x8} .. 0x{:x8}", this.src_channels, this.dst_queue.compression, start, end).flush();

        foreach (channel; this.src_channels)
        {
            this.records_count      = 0;
            this.dst_queue.setChannel(channel);

            this.initProgress(channel);

            this.sw.start();

            Stdout.format("\nChannel: {} [{} items]\nProgress: ", channel, this.channel_records).flush();

            if ( this.src.commandSupported(DhtConst.Command.GetRange) )
            {
                this.src.getRange(channel, start, end, &this.put).eventLoop();
            }
            else
            {
                this.src.getAll(channel, &this.put).eventLoop();
            }

            auto time = this.sw.stop();
            auto records_per_sec = time > 0 ? this.records_count / time : 0;
            auto bytes_per_record = this.records_count > 0 ? this.records_bytes / this.records_count : 0;
            Stdout.formatln("\n{,-22} {}s \n{,-22} {} \n{,-22} {} \n{,-22} {} \n{,-22} {}", 
                    "Time:",                    this.sw.stop(),
                    "Records copied:",          this.records_count,
                    "Records/s:",               records_per_sec,
                    "Bytes:",                   this.records_bytes,
                    "Avg. bytes per record:",   bytes_per_record).flush();

            this.dst_queue.flush();                                             // write everything which is still in the queue for this channel
        }
    }

    /***************************************************************************

        Method to calculatate node ranges based on the number of nodes
        with an even distritution.

        Params:
            number = number of nodes

        Returns:
            void

     **************************************************************************/

    public void range ( uint number )
    {
        auto range = hash_t.max / number;
        uint start;

        for ( uint i = 0; i < number - 1; i++ )
        {
            Stdout.formatln("{:X8} - {:X8}", start, start + range);
            start = start + range + 1;
        }

        Stdout.formatln("{:X8} - {:X8}", start, hash_t.max);
    }

    /***************************************************************************

        Checks if a channel is already set. If not grab all channels from 
        the source dht node cluster. 

        Params:

        Returns:
            void

     **************************************************************************/

    private void initChannels ()
    {
        if (this.src_channels.length == 0)
        {
            this.src.getChannels(&this.addChannels).eventLoop();
        }
    }

    /***************************************************************************

        Calculate number of channel records

        Params:
            address = node IP address
            port = node port
            channel = node channel name 
            records = number of records
            bytes = number of bytes

        Returns:
            void

     **************************************************************************/

    private void getChannelRecords ( DhtClient.RequestContext context, char[] address, ushort port, char[] channel, 
            ulong records, ulong bytes )
    {
        this.channel_records += records;
    }

    /***************************************************************************

        Calculate how many records are used to display a progress bar item

        Params:

        Returns:
            void

     **************************************************************************/

    private void initProgress ( char[] channel )
    {
        this.src.getChannelSize(channel, &this.getChannelRecords).eventLoop();

        if (this.channel_records > this.progress_steps)
        {
            this.progress_ = cast (uint) (this.channel_records / this.progress_steps);
        }
    }

    /***************************************************************************

        Add channels to internal channel list

        Params:
            id = internal dht id
            channel = channel name

        Returns:
            void

     **************************************************************************/

    private void addChannels ( DhtClient.RequestContext context, char[] channel )
    {
        bool found = false;

        if ( channel.length != 0 && !this.src_channels.contains(channel) )
        {
            this.src_channels.appendCopy(channel);
        }

        this.src_channels.sort;
    }

    /***************************************************************************

        Saves all channels that need to be copied

        Params:
            id = internal dhtclient id
            key = entry key
            value = entry value 

        Returns:
            void

     **************************************************************************/

    private void put ( DhtClient.RequestContext context, char[] key, char[] value )
    {
//        debug Stdout.formatln("ID: {}\t Key: {}\t Value: {}", id, key, value);

        if ( key.length )
        {
            this.dst_queue.put(DhtHash.straightToHash(key), value);

            this.records_bytes += value.length;
            this.records_count++;

            if ((this.records_count % this.progress_) == 0)
            {
                Stdout.format(".").flush();
            }
        }
    }

    /***************************************************************************

        Initializes the dht client connections

        Params:
            src_file = name of the source dht node cluster configuration
            dht_file = name of the destination dht node cluster configuration

        Returns:
            void

     **************************************************************************/

    private void initDhtClients ( in char[] src_file, in char[] dst_file )
    {
        this.src = new DhtClient(this.SRC_CONNECTIONS);
        this.dst = new DhtClient(this.DST_CONNECTIONS);
        this.dst_queue = new DestinationQueue(this.dst);

        this.src.error_callback = &this.handleError;
        this.dst.error_callback = &this.handleError;

        this.src.addNodes(src_file);
        this.dst.addNodes(dst_file);

        debug Stdout.formatln("Source: {} [{}]", src_file, this.src.nodeRegistry().length);
        debug Stdout.formatln("Destination: {} [{}]", dst_file, this.dst.nodeRegistry().length);

        this.src.nodeHandshake();
        this.dst.nodeHandshake();
    }

    /***************************************************************************

        Error handler for dht range command
        
        Params: 
            e = error info object
            
        Returns:
            void

     **************************************************************************/
    
    private void handleError ( DhtClient.ErrorInfo e )
    {
        Stdout.formatln("Error: {}", e.message).flush;
    }
}
