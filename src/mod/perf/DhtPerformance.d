/*******************************************************************************

    DHT performance test
    
    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved
    
    version:        Jun 2010: Initial release
    
    authors:        Gavin Norman
    
    --
    
    Displays an updating count of the number of records in each channel of the
    DHT node specified in the config file.
    
    Uses the channel "____test" to insert data and test on this.

 ******************************************************************************/

module mod.perf.DhtPerformance;



/*******************************************************************************

    Imports

 ******************************************************************************/

private import  ocean.text.Arguments;

private import  swarm.dht.DhtClient,
                swarm.dht.DhtHash,
                swarm.dht.DhtConst;

private import  swarm.dht.client.DhtNodesConfig;

private import  tango.time.StopWatch;

private import  Integer = tango.text.convert.Integer;

private import  tango.util.log.Trace;

private import  tango.math.random.Random;



/*******************************************************************************

    DhtPerformance - starts performance tests

 ******************************************************************************/

struct DhtPerformance
{
    public static bool run ( Arguments args )
    {
        scope worker = new DhtPerformanceWorker();
        
        if (args.getInt!(uint)("connections"))
        {
            worker.setNumberConnections(args.getInt!(uint)("connections"));
        }
        
        if (args.getInt!(uint)("iterations"))
        {
            worker.setNumberIterations(args.getInt!(uint)("iterations"));
        }
        
        if (args.getInt!(uint)("eventloop"))
        {
            worker.setPutMultiple(args.getInt!(uint)("eventloop"));
        }
        
        if (args.getInt!(uint)("size"))
        {
            worker.setValueSize(args.getInt!(uint)("size"));
        }
        worker.runTest();
        
        return true;
    }
}


/*******************************************************************************

    DhtPerformanceWorker - class

 ******************************************************************************/

class DhtPerformanceWorker 
{
    
    /***************************************************************************
     
         Test Channel
     
     **************************************************************************/
    
    private     char[]              channel                 = "____test";
    
    /***************************************************************************
    
        Number of progress steps shown while doing the test

     **************************************************************************/
    
    private     uint                progress_steps          = 30;
    
    /***************************************************************************
    
        Number that tells when to show the next progress step based on the 
        number of iteration and the numbmer of progress steps.s 
    
     **************************************************************************/
    
    private     uint                progress_               = 1;
    
    /***************************************************************************
     
         DhtClient
     
     **************************************************************************/
    
    private     DhtClient           dht;
    
    /***************************************************************************
    
        StopWatch
    
     **************************************************************************/
    
    private     StopWatch           sw;
    
    /***************************************************************************
    
       Number of connections to each dht node  
    
     **************************************************************************/
                     
    private     uint                connections             = 10;
    
    /***************************************************************************
    
        Number of items that are put on the stack before the eventloop is called  
     
     **************************************************************************/
    
    private     uint                eventloop_stack         = 200;
    
    /***************************************************************************
    
        Number of iterations for each test
     
     **************************************************************************/
    private     uint                number_iterations       = 10_000;
                     
    /***************************************************************************
    
        Size of the entry in bytes
     
     **************************************************************************/
        
    private     uint                value_size              = 400;
    
    /***************************************************************************
    
        Value that is written into the dht nodes
     
     **************************************************************************/
        
    private     char[]              value;
    
    /***************************************************************************
    
        Keys for random reads 

     **************************************************************************/
    
    private     hash_t[]            ran_seq_keys;
    
    /***************************************************************************
    
        Buffer used for inserting multiple items per eventloop
    
     **************************************************************************/
    
    private     char[][]            put_buffer;         
                         
    /***************************************************************************
    
        Constructor 
    
     **************************************************************************/
        
    public this () {}
    
    /***************************************************************************
    
        Set number of connections used to connect to each dht node
        
        Param: 
            number_connections = number of connection to each dht node
            
        Returns:
            void
    
     **************************************************************************/
        
    public void setNumberConnections ( uint number_connections )
    {
        this.connections = number_connections;
    }
    
    /***************************************************************************
    
        Set size of the value that should be written to the dht node
        
        Param: 
            value_size = size of the value of each entry
            
        Returns:
            void
    
     **************************************************************************/
    
    public void setValueSize ( uint value_size )
    {   
        this.value_size = value_size;
    }
    
    /***************************************************************************
    
        Set the number of iterations that should be done for each test
        
        Param: 
            number_iterations = number of iterations that should be done for each test
            
        Returns:
            void
    
     **************************************************************************/
        
    public void setNumberIterations ( uint number_iterations )
    {
        this.number_iterations = number_iterations;
    }
    
    /***************************************************************************
    
        Set the number of puts and gets that should be packed onto the
        eventloop stack.
        
        Param: 
            eventloop_stack = number of items that should be put on the eventloop stack
            
        Returns:
            void
    
     **************************************************************************/
        
    public void setPutMultiple ( uint eventloop_stack )
    {
        this.eventloop_stack = eventloop_stack;
    }
        
    /***************************************************************************
    
        Start the performance tests 
    
     **************************************************************************/
        
    public void runTest ()
    {
        this.initTest();
     
        this.putSingle();
        
        this.putMultiple();
                        
        this.readAll();
        
        this.readRandom();
        
        this.cleanUp();
        
        Trace.formatln("").flush();
    }
        
    /***************************************************************************
    
        Initializes the test parameter 
    
     **************************************************************************/
        
    private void initTest ()
    {
        this.initDhtClient();
        
        this.initRandomKeys();
        
        this.initValue();
        
        this.outputPerformanceTestConfiguration();
        
        if (this.number_iterations > this.progress_steps)        
            this.progress_ = this.number_iterations / this.progress_steps;
        
        this.put_buffer.length = this.eventloop_stack;
    }
    
    /***************************************************************************

        Initializes the dht client connections
                
        Returns:
            void

     **************************************************************************/
    
    private void initDhtClient ()
    {
        this.dht = new DhtClient(this.connections);
        
        DhtNodesConfig.addNodesToClient(this.dht, "etc/dhtnodes.xml");
        
        this.dht.queryNodeRanges().eventLoop();
    }
    
    /***************************************************************************

        Initializes random keys for the performance test
                
        Returns:
            void
    
     **************************************************************************/
        
    private void initRandomKeys ()
    {
        uint rand;
        
        scope random = new Random();
        
        for (uint i=0; i < this.number_iterations; i++)
        {
            random(rand);
            
            debug Trace.formatln("Rand: {}", rand).flush();
        
            this.ran_seq_keys ~= rand;
        }
    }
    
    /***************************************************************************

        Initializes value that should be written into the dht with the 
        correct size
                
        Returns:
            void
    
     **************************************************************************/
        
    private void initValue ()
    {
        for (uint i = 0; i<this.value_size; i++)
        {
            this.value ~= "a".dup;
        }
    }
    
    /***************************************************************************

        Write into dht and call the eventloop after each put. 
                
        Returns:
            void
    
     **************************************************************************/
        
    private void putSingle ()
    {
        uint count = 0;
        
        Trace.format("{,25}", "Put Single test").flush();
        
        this.sw.start();
        
        for (uint i=0; i < this.number_iterations; i++)
        {
            this.dht.put(this.channel, i, this.value);
            this.dht.eventLoop();
            
            if ((i%this.progress_) == 0)
            {
                Trace.format(".").flush();
            }
        }
        
        Trace.formatln(" Items written: {,9}\tItems per second: {,10}\t Time: {,6}s",
                this.number_iterations, this.number_iterations/this.sw.stop(),this.sw.stop());
    }
    
    /***************************************************************************

        Write into dht and call event loop after defined steps
                
        Returns:
            void
    
     **************************************************************************/
        
    private void putMultiple ()
    {
        uint count = 0;
        
        Trace.format("{,25}", "Put Mulitple test").flush();
           
        this.sw.start();
        
        for (uint i=0; i < this.number_iterations; i++)
        {   
            this.put_buffer[count] = this.value;
            this.dht.put(this.channel, i, this.put_buffer[count]);
            
            if (count == (this.eventloop_stack - 1))
            {             
                this.dht.eventLoop();
                count = 0;
            }
            
            if ((i%this.progress_) == 0)
            {
                Trace.format(".").flush();
            }
            
            count++;
        }
        
        this.dht.eventLoop();
        
        Trace.formatln(" Items written: {,9}\tItems per second: {,10}\t Time: {,6}s",
                this.number_iterations, this.number_iterations/this.sw.stop(),this.sw.stop());
    }
        
    /***************************************************************************

        Read all items from the test dht channel
                
        Returns:
            void

     **************************************************************************/
    
    private void readAll ()
    {
        uint count = 0;
        
        this.sw.start();
        
        Trace.format("{,25}", "Read all test").flush();
        
        this.dht.getAll(this.channel, ( hash_t id, char[] key, char[] value )
                {
                    if ((count%this.progress_) == 0)
                    {
                        Trace.format(".").flush();
                    }
                    count++;
                });
        this.dht.eventLoop();
        
        Trace.formatln(" Items read: {,12}\tItems per second: {,10}\t Time: {,6}s",
                this.number_iterations, this.number_iterations/this.sw.stop(),this.sw.stop());
    }
    
    /***************************************************************************

        Read random from the test dht channel
                
        Returns:
            void
    
     **************************************************************************/
        
    private void readRandom ()
    {
        uint count = 0;
        
        char[] _value;
        
        this.sw.start();
        
        Trace.format("{,25}", "Write for random test").flush();
        
        foreach (i, id; this.ran_seq_keys)
        {   
            this.put_buffer[count] = this.value;
            this.dht.put(this.channel, id, this.put_buffer[count]);
                        
            if (count == (this.eventloop_stack - 1))
            {   
                this.dht.eventLoop();
                count = 0;
            }   
            
            if ((i%this.progress_) == 0)
            {
                Trace.format(".").flush();
            }
            
            count++;
        }     
        
        this.dht.eventLoop();
        
        Trace.formatln(" Items written: {,9}\tItems per second: {,10}\t Time: {,6}s",
                this.number_iterations, this.number_iterations/this.sw.stop(),this.sw.stop());
        
        count = 0;
        
        Trace.format("{,25}", "Read random test").flush();
        
        this.sw.start();
        
        foreach (i; this.ran_seq_keys)
        {
            this.dht.get(this.channel, i, _value);
            this.dht.eventLoop();
            
            if ((count%this.progress_) == 0)
            {
                Trace.format(".").flush();
            }
            count++;
        }
        
        Trace.formatln(" Items read: {,12}\tItems per second: {,10}\t Time: {,6}s", 
                this.number_iterations, this.number_iterations/this.sw.stop(),this.sw.stop());
    }
    
    /***************************************************************************

        Remove test data
                
        Returns:
            void
    
     **************************************************************************/
        
    private void cleanUp ()
    {
        Trace.formatln("\nCleanup.").flush();
        
        for (uint i=0; i<this.number_iterations; i++)
        {
            this.dht.remove(this.channel, i);
            this.dht.eventLoop();
        }
        
        foreach (i; this.ran_seq_keys)
        {
            this.dht.remove(this.channel, i);
            this.dht.eventLoop();
        }
    } 
    
    /***************************************************************************

        Output dht client and performance test settings 
                
        Returns:
            void
    
     **************************************************************************/
        
    private void outputPerformanceTestConfiguration ()
    {
        Trace.formatln("").flush();
        Trace.formatln("Number of iterations:           {}", this.number_iterations).flush();
        Trace.formatln("Number of dht node connections: {}", this.connections).flush();
        Trace.formatln("Value size:                     {}", this.value_size).flush();
        Trace.formatln("Dht eventloop stack:            {}\n", this.eventloop_stack).flush();
    }
}
