/*******************************************************************************

    DHT node copy 

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:        October 2010: Initial release

    authors:        Lars Kirchhoff

    --

    Copies the data of all channels from a source dht node cluster to the 
    destination dht node cluster.
     
 ******************************************************************************/

module mod.copy.DhtCopy;



/*******************************************************************************

    Imports

 ******************************************************************************/

private import  ocean.text.Arguments;

private import  swarm.dht.DhtClient,
                swarm.dht.DhtHash,
                swarm.dht.DhtConst;

private import  swarm.dht.client.DhtNodesConfig;

private import  tango.io.Stdout;



/*******************************************************************************

    DhtCopy - starts the copy worker process

 ******************************************************************************/

struct DhtCopy
{   
    public static bool run ( Arguments args )
    {
        scope worker = new DhtCopyWorker();
    
        if (args.getString("source").length != 0 && 
            args.getString("destination").length != 0)
        {
            worker.copy(args.getString("source"), args.getString("destination"));
            return true;
        }
        else
        {
            worker.range(args.getInt!(uint)("number"));
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
    
    private     const uint          CONNECTIONS = 10;
    
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
    
    private     uint                count;
    
    /***************************************************************************
     
        Constructor
     
     **************************************************************************/
    
    public this () {}
        
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
        
        this.src.getChannels(( hash_t id, char[] channel )
                {   
                    this.src_channels ~= channel.dup;     
                }).eventLoop();            
        
        foreach (channel; this.src_channels)
        {
            this.count              = 0;
            this.eventloop_count    = 0;
            this.current_channel    = channel;
            
            try
            {
                this.src.getAll(channel, &this.copyChannel).eventLoop();
            }
            catch (Exception e) {}
        }
    }
    
    /***************************************************************************

        Saves all channels that need to be copied
                
        Params:
            
        Returns:
            void
    
     **************************************************************************/
    
    private void copyChannel ( hash_t id, char[] key, char[] value )
    {
        debug Stdout.formatln("ID: {}\t Key: {}\t Value: {}", id, key, value);
        
        this.put_buffer[this.eventloop_count] = value.dup;
        this.dst.put(this.current_channel, key, this.put_buffer[this.eventloop_count]);
        
        this.eventloop_count++;
        
        if (this.eventloop_count == 200)
        {
            this.dst.eventLoop();
            this.eventloop_count = 0;
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
        this.src = new DhtClient(10);
        this.dst = new DhtClient(this.CONNECTIONS);
                   
        DhtNodesConfig.addNodesToClient(this.src, src_file);
        DhtNodesConfig.addNodesToClient(this.dst, dst_file);
        
        debug Stdout.formatln("Source: {} [{}]", src_file, this.src.nodeRegistry().length);
        debug Stdout.formatln("Destination: {} [{}]", dst_file, this.dst.nodeRegistry().length);
        
        this.src.queryNodeRanges().eventLoop();
        this.dst.queryNodeRanges().eventLoop();
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
        uint start = 0, end = 0;
       
        for (uint i = 0; i < number; i++)
        {
            end = start + range;
            Stdout.formatln("{:X8} - {:X8}", start, end);
            start = end + 1;
        }
    }
}
