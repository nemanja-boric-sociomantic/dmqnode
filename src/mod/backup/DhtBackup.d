/*******************************************************************************

    DHT Backup

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        October 2010: Initial release

    authors:        Lars Kirchhoff

    --

    Simple backup tool that copies the content of a source dht node cluster
    to a local dump file that can be imported again.

 ******************************************************************************/

module mod.copy.DhtBackup;



/*******************************************************************************

    Imports

 ******************************************************************************/

private import  core.dht.DestinationQueue;

private import  ocean.core.Array;

private import  ocean.text.Arguments;

private import  swarm.dht.DhtClient,
                swarm.dht.DhtHash,
                swarm.dht.DhtConst;

private import  swarm.dht.client.connection.ErrorInfo,
                swarm.dht.client.DhtNodesConfig;

private import  tango.core.Array;

private import  Integer = tango.text.convert.Integer;

private import  tango.io.Stdout;

private import  tango.math.Math;

private import  tango.io.device.File,
                tango.io.stream.Buffered;

private import  tango.time.StopWatch;



/*******************************************************************************

    DhtCopy - starts the copy worker process

 ******************************************************************************/

struct DhtBackup
{
    public static bool run ( Arguments args )
    {
        scope worker = new DhtBackupWorker();

        if (args.getString("source").length != 0 &&
            !args.getBool("list"))
        {
            if (args.getString("channel").length != 0)
            {
                worker.setChannel(args.getString("channel"));
            }

            if (args.getString("output").length != 0)
            {
                worker.setOutputFile(args.getString("output"));
                worker.dhtBackup(args.getString("source"));
            }

            if (args.getBool("dump_all") &&
                args.getString("output_dir").length != 0)
            {
                worker.setOutputDir(args.getString("output_dir"));
                worker.dhtBackup(args.getString("source"));
            }

            if (args.getString("input").length != 0)
            {
                worker.setInputFile(args.getString("input"));
                worker.dhtImport(args.getString("source"));
            }

            return true;
        }

        if (args.getBool("list") && args.getString("source").length != 0)
        {
            worker.list(args.getString("source"), args.getString("destination"));
            return true;
        }

        return false;
    }
}



class DhtBackupWorker
{
    static              const                       IOBufferSize = 0x10000;     // 64k

    /***************************************************************************

        Number of connections to each DHT node

     **************************************************************************/

    private     const uint          SRC_CONNECTIONS = 1;

    /***************************************************************************

        Record queue the destination dht nodes (to put in batches)

     **************************************************************************/

    private     DestinationQueue    dst_queue;

    /***************************************************************************

        Dht client for the source nodes

     **************************************************************************/

    private     DhtClient           src;

    /***************************************************************************

        Names of source channels

     **************************************************************************/

    private     char[][]            src_channels;

    /***************************************************************************

        Name of current channel

     **************************************************************************/

    private     char[]              current_channel;

    /***************************************************************************

        Name of output file

     **************************************************************************/

    private     char[]              output_filename;
    private     char[]              _output_filename;

    /***************************************************************************

        Name of output dir

     **************************************************************************/

    private     char[]              output_dir;

    /***************************************************************************

        Name of input file

     **************************************************************************/

    private     char[]              input_filename;

    /***************************************************************************

        OutputBuffer

     **************************************************************************/

    private     BufferedOutput      output_buffer;
    
    /***************************************************************************

        InputBuffer
    
     **************************************************************************/
    
    private     BufferedInput      input_buffer;

    /***************************************************************************

        File instance

     **************************************************************************/

    private     File                file;

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

        Set output file name

        Params:
            filename = filename of output file

        Returns:
            void

     **************************************************************************/

    public void setOutputFile ( char[] filename )
    {
        this.output_filename = filename;
    }

    /***************************************************************************

        Set output directory

        Params:
            dir = directory for output files

        Returns:
            void

     **************************************************************************/

    public void setOutputDir ( char[] dir )
    {
        this.output_dir = dir;
    }

    /***************************************************************************

        Set input file name

        Params:
            filename = filename of input file

        Returns:
            void

     **************************************************************************/

    public void setInputFile ( char[] filename )
    {
        this.input_filename = filename;
    }

    /***************************************************************************

        Backups the data from either a channel or from all channels of
        a dht node cluster.

        Params:
            src_file = name of the source dht node cluster configuration

        Returns:
            void

     **************************************************************************/

    public void dhtBackup ( char[] src_file )
    {
        this.initDhtClients(src_file);

        this.initChannels();

        debug Stdout.formatln("Channels: {}\nCompression: {}\nRange: 0x{:x8} .. 0x{:x8}", this.src_channels, this.dst_queue.compression, start, end).flush();

        foreach (channel; this.src_channels)
        {
            this.records_count = 0;

            this.openOutputBuffer(channel);

            this.initProgress(channel);

            this.sw.start();

            Stdout.format("\nChannel: {} [{} items]\nProgress: ", channel, this.channel_records).flush();

            this.writeValue!(ulong)(this.output_buffer, this.channel_records);  // write number of records to backup dump

            this.src.getAll(channel, &this.dumpData).eventLoop();               // write data to backup dump

            auto time = this.sw.stop();
            auto records_per_sec = time > 0 ? this.records_count / time : 0;
            auto bytes_per_record = this.records_count > 0 ? this.records_bytes / this.records_count : 0;
            Stdout.formatln("\n{,-22} {}s \n{,-22} {} \n{,-22} {} \n{,-22} {} \n{,-22} {}",
                    "Time:",                    this.sw.stop(),
                    "Records copied:",          this.records_count,
                    "Records/s:",               records_per_sec,
                    "Bytes:",                   this.records_bytes,
                    "Avg. bytes per record:",   bytes_per_record).flush();

            this.closeOutputBuffer();
        }
    }

    /***************************************************************************

        Imports the data from a channel into a dht node cluster.
    
        Params:
            src_file = name of the source dht node cluster configuration
    
        Returns:
            void
    
     **************************************************************************/

    public void dhtImport ( char[] src_file )
    {
        hash_t key_;
        
        this.initDhtClients(src_file);
        
        this.openInputBuffer();
        
        foreach (channel; this.src_channels)
        {
            Stdout.formatln("Channel: {}", channel).flush();
            
            this.dst_queue.setChannel(channel);
            
            ulong num_records = this.readValue!(ulong)(this.input_buffer);
            
            Stdout.formatln("Number records: {}", num_records).flush();
            
            scope key = new char[8];
            scope val = new char[0x400];
            
            for (size_t i = 0; i < num_records; i++)
            {
                key_ = DhtHash.straightToHash(this.readString(this.input_buffer, key));
                
                this.dst_queue.put(key_, this.readString(this.input_buffer, val));
            }
            
            this.dst_queue.flush();
        }
        
        this.closeInputBuffer();
    }

    /***************************************************************************

        Outputs a list with channels in the source nodes

        Params:

        Returns:
            void

     **************************************************************************/

    public void list ( char[] src_file, char[] dst_file )
    {
        this.initDhtClients(src_file);

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

    private void addChannels ( hash_t id, char[] channel )
    {
        bool found = false;

        if ( channel.length != 0 && !this.src_channels.contains(channel) )
        {
            this.src_channels.appendCopy(channel);
        }

        this.src_channels.sort;
    }

    /***************************************************************************

        Dumps the data to the output file

        Params:
            id = internal dhtclient id
            key = entry key
            value = entry value

        Returns:
            void

     **************************************************************************/

    private void dumpData ( hash_t id, char[] key, char[] value )
    {
        // debug Stdout.formatln("ID: {}\t Key: {}\t Value: {}", id, key, value);
        
        if ( value.length )
        {
            this.writeString(this.output_buffer, key);
            this.writeString(this.output_buffer, value);

            this.records_bytes += value.length;
            this.records_count++;

            if ((this.records_count % this.progress_) == 0)
            {
                Stdout.format(".").flush();
            }
        }
    }

    /***************************************************************************

        Opens the output buffer for the output file where the data should
        be dump to

        Params:
            channel = channel name

        Returns:
            void

     **************************************************************************/

    private void openOutputBuffer ( char[] channel )
    {
        if (this.output_filename.length != 0)
        {
            this._output_filename = this.output_filename;
        }
        else
        {
            this._output_filename = this.output_dir ~ channel ~ ".dhtb";
        }

        this.file = new File();
        this.file.open(this._output_filename, this.file.WriteCreate);

        this.output_buffer = new BufferedOutput(this.file, this.IOBufferSize);
    }
    
    /***************************************************************************
    
        Opens the input buffer for the input file where the data should
        be read from 
            
        Returns:
            void
    
     **************************************************************************/
    
    private void openInputBuffer ( )
    {
        this.file = new File();
        
        this.file.open(this.input_filename, this.file.ReadExisting);
    
        this.input_buffer = new BufferedInput(this.file, this.IOBufferSize);
    }

    /***************************************************************************

        Close output buffer and output file

        Returns:
            void

     **************************************************************************/

    private void closeOutputBuffer ( )
    {
         this.output_buffer.flush();
         this.file.close();
    }
    
    /***************************************************************************

        Close input buffer and input file
    
        Returns:
            void
    
     **************************************************************************/
    
    private void closeInputBuffer ( )
    {
         this.input_buffer.flush();
         this.file.close();
    }

    /***************************************************************************

        Initializes the dht client connections

        Params:
            src_file = name of the source dht node cluster configuration
            dht_file = name of the destination dht node cluster configuration

        Returns:
            void

     **************************************************************************/

    private void initDhtClients ( in char[] src_file )
    {
        this.src = new DhtClient(this.SRC_CONNECTIONS);
        this.src.error_callback = &this.handleError;
        DhtNodesConfig.addNodesToClient(this.src, src_file);
        
        this.dst_queue = new DestinationQueue(this.src);

        debug Stdout.formatln("Source: {} [{}]", src_file, this.src.nodeRegistry().length);

        this.src.nodeHandshake();
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

        Reads a value from input. In particular, T.sizeof bytes of raw data are
        read from input and then casted to T.

        Params:
            input = BufferedInput producing serialized raw data to be casted to
                    type T

        Returns:
            resulting value

     **************************************************************************/

    private static T readValue ( T ) ( BufferedInput input )
    {
        T result;

        input.fill((cast (void*) &result)[0 .. T.sizeof], true);

        return result;
    }

    /***************************************************************************

        Reads a string (char[]) from input. In particular, first the string
        length  read as a serialized size_t value, then the string content is
        read and str populated with it.

        Params:
            input = BufferedInput producing serialized string raw data
            str   = destination string instance

        Returns:
            resulting string

     **************************************************************************/

    private static char[] readString ( BufferedInput input, ref char[] str )
    {
        str.length = readValue!(size_t)(input);

        input.fill(str, true);

        return str;
    }

    /***************************************************************************

        Writes a value to output. In particular, the serialized raw data of
        value are written, which have the length of T.sizeof bytes.


        Params:
            output = BufferedOutput consuming serialized raw data that can later
                     to be casted back to type T by readValue()
            value  = value to write

        Returns:
            number of bytes written

     **************************************************************************/

    private static size_t writeValue ( T ) ( BufferedOutput output, T value )
    {
        return output.write((cast (void*) &value)[0 .. T.sizeof]);
    }

    /***************************************************************************

        Writes str to output. In particular, first str.length is written as a
        serialized size_t value, then the content of str.

        Params:
            output = BufferedOutput consuming serialized raw data that can later
                     to be casted back to type T by readValue()
            str    = string to write

        Returns:
            number of bytes written

     **************************************************************************/

    private static size_t writeString ( BufferedOutput output, char[] str )
    {
        size_t bytes = writeValue(output, str.length);

        bytes += output.write(str);

        return bytes;
    }
}