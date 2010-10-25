/*******************************************************************************

    DHT node copy 

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        October 2010: Initial release

    authors:        Lars Kirchhoff

    --

    Copies the data of all channels from a source dht node cluster to the 
    destination dht node cluster.

    TODO: 
    1. handle compression properly
    2. handle get and put method properly
    3. use streaming instead of single puts 

 ******************************************************************************/

module mod.copy.DhtCopy;



/*******************************************************************************

    Imports

 ******************************************************************************/

private import  ocean.text.Arguments;

private import  swarm.dht.DhtClient,
                swarm.dht.DhtHash,
                swarm.dht.DhtConst;

private import  swarm.dht.client.connection.ErrorInfo,
                swarm.dht.client.DhtNodesConfig;

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
            args.getString("destination").length != 0 && !args.getBool("list"))
        {
            if (args.getString("channel").length != 0)
            {
                worker.setChannel(args.getString("channel"));
            }

            if (args.getBool("compression"))
            {
                worker.setCompression(true);
            }

            worker.copy(args.getString("source"), args.getString("destination"));

            return true;
        }

        if (args.getBool("range"))
        {
            worker.range(args.getInt!(uint)("number"));
            return true;
        }

        if (args.getBool("list") && 
            args.getString("source").length != 0 && 
            args.getString("destination").length != 0)
        {
            worker.list(args.getString("source"), args.getString("destination"));
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

        Put buffer for multiple items per eventloop 

     **************************************************************************/

    private     char[][200]         put_buffer;

    /***************************************************************************

        PutMethod delegate

     **************************************************************************/

    private     void delegate (char[], char[], ref char[], bool) put_method_dg;    

    /***************************************************************************

        Eventloop counter for the put buffer 

     **************************************************************************/

    private     uint                eventloop_count;

    /***************************************************************************

        Current channel that is being copied

     **************************************************************************/

    private     char[]              current_channel;

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
        number of iteration and the numbmer of progress steps.s 

     **************************************************************************/

    private     uint                progress_               = 1;

    /***************************************************************************

        Should compression be used for the destination node channel

     **************************************************************************/

    private     bool                compression             = false; 

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
        this.compression = compression;
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
        
    public void copy ( in char[] src_file, in char[] dst_file )
    {
        this.initDhtClients(src_file, dst_file);

        this.initChannels();

        debug Stdout.formatln("Channels: {}\n Compression: {}", this.src_channels, this.compression).flush();

        foreach (channel; this.src_channels)
        {
            this.records_count      = 0;
            this.eventloop_count    = 0;
            this.current_channel    = channel.dup;

            this.initProgress();

            this.sw.start();

            Stdout.format("Channel: {} [{} items]\nProgress: ", this.current_channel, this.channel_records).flush();

            this.src.getRange(this.current_channel, hash_t.min, hash_t.max, &this.copyChannel).eventLoop();

            Stdout.formatln("\n{,-22} {}s \n{,-22} {} \n{,-22} {} \n{,-22} {} \n{,-22} {}", 
                    "Time:",                    this.sw.stop(),
                    "Records copied:",          this.records_count,
                    "Records/s:",               this.records_count/this.sw.stop(),
                    "Bytes:",                   this.records_bytes,
                    "Avg. bytes per record:",   this.records_bytes/this.records_count).flush();
        }

        this.dst.eventLoop();                                                   // final eventloop to write everything which is still in the eventloop stack
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

        Outputs a list with channels in the source nodes

        Params:

        Returns:
            void

     **************************************************************************/

    public void list ( char[] src_file, char[] dst_file )
    {
        this.initDhtClients(src_file, dst_file);

        this.src.getChannels(&this.addChannels).eventLoop();

        foreach (channel; this.src_channels)
        {
            Stdout.formatln(channel).flush();
        }
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

    private void getChannelRecords ( uint id, char[] address, ushort port, char[] channel, 
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

    private void initProgress ()
    {
        this.src.getChannelSize(this.current_channel, &this.getChannelRecords).eventLoop();

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

    private void addChannels ( hash_t id, char[] channel )
    {
        bool found = false;

        if (channel.length != 0)
        {
            foreach (_channel; this.src_channels)
            {
                if (_channel == channel) 
                {
                    found = true;
                    break;
                }
            }

            if (!found) this.src_channels ~= channel.dup;
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

    private void copyChannel ( hash_t id, char[] key, char[] value )
    {
        debug Stdout.formatln("ID: {}\t Key: {}\t Value: {}", id, key, value);

        if (key.length)
        {
            this.put_buffer[this.eventloop_count] = value.dup;

            this.records_bytes += value.length;

            this.put_method_dg(this.current_channel, key, this.put_buffer[this.eventloop_count], this.compression);

            this.eventloop_count++;

            if (this.eventloop_count == 200)
            {
                this.dst.eventLoop();
                this.eventloop_count = 0;
            }

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

        DhtNodesConfig.addNodesToClient(this.src, src_file);
        DhtNodesConfig.addNodesToClient(this.dst, dst_file);

        debug Stdout.formatln("Source: {} [{}]", src_file, this.src.nodeRegistry().length);
        debug Stdout.formatln("Destination: {} [{}]", dst_file, this.dst.nodeRegistry().length);

        this.src.queryNodeRanges().eventLoop();
        this.dst.queryNodeRanges().eventLoop();

        this.getPutMethod();

        this.src.error_callback = &this.handleError;
        this.dst.error_callback = &this.handleError;
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

    /***************************************************************************

        Get Available put method. Sets the internal put method delegate.

        Default is put. Fallback is putDup.

        Params:

        Returns:
            void

     **************************************************************************/

    private void getPutMethod ()
    {
        char[] channel  = "____test";
        char[] value    = "1";
        hash_t key      = 1;

        this.put_method_dg = &this.put;

        this.dst.error_callback(( ErrorInfo e )
                {
                    debug Stdout.formatln("Method: putDup").flush();
                    this.put_method_dg = &this.putDup;
                });

        this.dst.put(channel, key, value).eventLoop();

        this.dst.remove(channel, key).eventLoop();
    }

    /***************************************************************************

        Simple put method wrapper.

        Params:
            channel = channel name 
            key = key
            value = value
            compress = compression on/off

        Returns:
            void

     **************************************************************************/

    private void put ( char[] channel, char[] key, ref char[] value, bool compress = false )
    {
        this.dst.put(channel, DhtHash.straightToHash(key), value, compress);
    }

    /***************************************************************************

        Simple putDup method wrapper.

        Params:
            channel = channel name 
            key = key
            value = value
            compress = compression on/off

        Returns:
            void

     **************************************************************************/

    private void putDup ( char[] channel, char[] key, ref char[] value, bool compress = false )
    {
        this.dst.putDup(channel, DhtHash.straightToHash(key), value, compress);
    }   
}
