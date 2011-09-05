/*******************************************************************************

    Queue command tests

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.Commands;

/*******************************************************************************

    Swarm Imports

*******************************************************************************/

private import swarm.queue.QueueClient,
               swarm.queue.QueueConst;

/*******************************************************************************

    Ocean Imports

*******************************************************************************/

private import ocean.util.container.RingQueue,
               ocean.io.select.EpollSelectDispatcher, 
               ocean.util.log.Trace;

/*******************************************************************************

    Tango Imports

*******************************************************************************/

private import tango.math.random.Random;

private import Integer = tango.text.convert.Integer;

/*******************************************************************************

    Constants
    
    TODO: move them to cmd line parameters

*******************************************************************************/

const QueueSize = 10 * 1024 * 1024;  // 10mb
const QueuePushMultiNum = 3;
const QueueMaxPushSize = 5;


/*******************************************************************************

    News and returns an array of instances for each command of

    * push
    * pushCompressed
    * pushMulti
    * pushMultiCompressed

*******************************************************************************/

ICommand[] getCommands ( )
{
    return [cast(ICommand)new Push(QueueSize), 
            //new PushCompressed(QueueSize),
            new PushMulti(QueuePushMultiNum, QueueSize)
            //new PushMultiCompressed(QueuePushMultiNum, QueueSize)
           ];
}

/*******************************************************************************

    Interface for a command.
    
    Provides a unified way to run any of the push commands:
        
    * push
    * pushCompressed
    * pushMulti
    * pushMultiCompressed
    
    Additionally, it provides a pop and a consume method.
    Each call to push writes also to a local queue and each pop/consume checks
    its result with the local queue and throws an exception if it doesn't
    match.

*******************************************************************************/

abstract class ICommand
{   
    /***************************************************************************
    
        Pushes a test entry to the remote and local queue
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            channel         = channel to write to
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
    ***************************************************************************/
    
    void push ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                char[] channel, size_t amount = 1, 
                QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok );
        
    /***************************************************************************
    
        Pops an entry from the remote and local queue and compares the result.
        Throws if the values don't match.
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            channel         = channel to write to
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
         Returns:
             amount of popped entries
             
    ***************************************************************************/
    
    size_t pop ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                 char[] channel, size_t amount = 1, 
                 QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok );
        
    /***************************************************************************
    
        Consumes entries from the remote and local queue and compares the results.
        Throws if the values don't match.
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            channel         = channel to write to
            expected_result = optional, expected result code, defaults to Ok            
             
    ***************************************************************************/
    
    void consume ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                   char[] channel, 
                   QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok );
    
    /***************************************************************************
    
        Name of this command
        
        Returns:
            name of this command
             
    ***************************************************************************/
    
    char[] name ();
    
    /***************************************************************************
    
        Returns:
            Whether all commands that have been pushed have been popped/consumed
             
    ***************************************************************************/
    
    bool done();
        
    /***************************************************************************
    
        Info struct of the last request
        
    ***************************************************************************/
    
    QueueClient.RequestFinishedInfo info;
    
    /***************************************************************************
    
        request finished dg. Sets the info struct.
             
    ***************************************************************************/
    
    protected void requestFinished ( QueueClient.RequestFinishedInfo info )
    {
        this.info = info;
    }  
    
    /***************************************************************************
    
        creates a random amount of bytes
             
    ***************************************************************************/
    
    protected ubyte[] getRandom ( ubyte[] data )
    {
        int i = rand.uniformR(QueueMaxPushSize) + 1;
        
        foreach (ref b; data[0 .. i]) 
        {
            b = rand.uniformR2(ubyte.min, ubyte.max);
        }
       
        return data[0 .. i];
    }
}


class Push : ICommand
{        
    /***************************************************************************
    
        Local Queue for validation
    
    ***************************************************************************/
    
    protected ByteRingQueue local_queue;
           
    /***************************************************************************
    
        Constructor
        
        Params:
            size   = size of the local test queue in bytes
    
    ***************************************************************************/
    
    this ( size_t size )
    {
        this.local_queue = new ByteRingQueue(size);
    }
        
    /***************************************************************************
    
        Pushes a test entry to the remote and local queue
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            channel         = channel to write to
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
    ***************************************************************************/
    
    override void push ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                         char[] channel, size_t amount = 1, 
                         QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {        
        queue_client.requestFinishedCallback(&this.requestFinished);
        ubyte[QueueMaxPushSize] data = void;
        
        do synchronized (this.local_queue)
        {               
            auto rdata = getRandom(data);
            
            char[] pusher ( uint id )
            {
                return cast(char[]) rdata;
            }
            
            queue_client.push(channel, &pusher);
            
            epoll.eventLoop;            
            
            if (info.status != expected_result)
            {
                Trace.formatln("push failed, expected status {}, got {}: {}", 
                               expected_result, info.status, info.message);
                
                throw new Exception("Unexpected result");
            }       
            
            if (!this.local_queue.push(rdata))
            {
                throw new Exception("Local queue is full!");
            }
        }
        while (--amount > 0) 
    }
        
    /***************************************************************************
    
        Pops an entry from the remote and local queue and compares the result.
        Throws if the values don't match.
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            channel         = channel to write to
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
         Returns:
             amount of popped entries
             
    ***************************************************************************/
    
    override size_t pop ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                          char[] channel, size_t amount = 1, 
                          QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {
        bool error = false;
        queue_client.requestFinishedCallback(&this.requestFinished);
        
        do
        {       
            ubyte[] data;            
            synchronized (this.local_queue) 
            {
                data = this.local_queue.pop();
                
                void popper ( uint, char[] value )
                {
                    if (data != cast(ubyte[]) value)
                    {
                        Trace.formatln("Error: popped value {} differs from "
                                       "pushed value {}", cast(ubyte[]) value,
                                       data);
                        
                        error = true;
                    }
                }
                
                queue_client.pop(channel, &popper);            
                
                epoll.eventLoop;
            }
            
            if (info.status != expected_result)
            {
                Trace.formatln("pop failed, expected status {}, got {}: {}", 
                               expected_result, info.status, info.message);
                
                throw new Exception("Unexpected result");
            }
            
            if (error) throw new Exception("Error while popping");
        }
        while (--amount > 0)
            
        return amount;
    }
        
    /***************************************************************************
    
        Consumes entries from the remote and local queue and compares the results.
        Throws if the values don't match.
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            channel         = channel to write to
            expected_result = optional, expected result code, defaults to Ok            
             
    ***************************************************************************/
    
    override void consume ( EpollSelectDispatcher epoll, 
                            QueueClient queue_client, char[] channel, 
                            QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {
        queue_client.requestFinishedCallback(&this.requestFinished);
        
        void consumer ( uint id, char[] value )
        {
            ubyte[] data;
            synchronized (this.local_queue) 
            {
                data = this.local_queue.pop();
            
                if (data != cast(ubyte[]) value)
                {
                    Trace.formatln("Error: consumed value {} differs from "
                                   "pushed value {}", cast(ubyte[]) value,
                                   data);
    
                    epoll.shutdown;
                }
            }
        }
        
        queue_client.consume(channel, 1, &consumer);
        
        epoll.eventLoop;        
                
        if (info.status != expected_result)
        {
            Trace.formatln("consume failed, expected status {}, got {}: {}", 
                           expected_result, info.status, info.message);
            
            throw new Exception("Unexpected result");
        }
    }   
        
    /***************************************************************************
    
        Name of this command
        
        Returns:
            name of this command
             
    ***************************************************************************/
    
    char[] name ( )
    {
        return "push";
    }
   
    /***************************************************************************
    
        Returns:
            Whether all commands that have been pushed have been popped/consumed
             
    ***************************************************************************/
    
    bool done()
    {
        return this.local_queue.length == 0;
    }
    
    
    
}

class PushCompressed : Push
{     
           
    /***************************************************************************
    
        Constructor
        
        Params:
            size   = size of the local test queue in bytes
    
    ***************************************************************************/
    
    this ( size_t size )
    {
        super(size);
    }  
    
    /***************************************************************************
    
        Pushes a test entry to the remote and local queue
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            channel         = channel to write to
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
    ***************************************************************************/
    
    override void push ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                         char[] channel, size_t amount = 1, 
                         QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {
        queue_client.requestFinishedCallback(&this.requestFinished);
        ubyte[QueueMaxPushSize] data = void;
        
        do synchronized (this.local_queue) 
        {               
            auto rdata = getRandom(data);
             
            char[] pusher ( uint id )
            {
                return cast(char[]) rdata;
            }
            
            queue_client.pushCompressed(channel, &pusher);
            
            epoll.eventLoop;        
            
            if (info.status != expected_result)
            {
                Trace.formatln("pushCompressed failed, expected status {}, got {}: {}", 
                               expected_result, info.status, info.message);
                
                throw new Exception("Unexpected result");
            }
                        
            if (!this.local_queue.push(rdata))
            {
                throw new Exception("Local queue is full!");
            }
        }
        while (--amount > 0) 
    }
            
    /***************************************************************************
    
        Name of this command
        
        Returns:
            name of this command
             
    ***************************************************************************/
    
    char[] name ( )
    {
        return "pushCompressed";
    }
}


class PushMulti : ICommand
{        
    /***************************************************************************
    
        Local Queue for validation
    
    ***************************************************************************/
    
    protected ByteRingQueue[] local_queues;
       
    /***************************************************************************
    
        Amount of channels
    
    ***************************************************************************/
    
    protected size_t num_channels;
    
    /***************************************************************************
    
        Constructor
        
        Params:
            size   = size of the local test queue in bytes
    
    ***************************************************************************/
    
    this ( size_t num_channels, size_t size )
    {
        this.local_queues = new ByteRingQueue[num_channels];

        foreach (ref queue; this.local_queues)
        {
            queue = new ByteRingQueue(size);
        }
        
        this.num_channels = num_channels;
    }
        
    /***************************************************************************
    
        Pushes a test entry to the remote and local queue
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            channel         = channel to write to
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
    ***************************************************************************/
    
    override void push ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                         char[] channel, size_t amount = 1, 
                         QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {
        scope char[][] channels = new char[][num_channels];
        queue_client.requestFinishedCallback(&this.requestFinished);
        ubyte[QueueMaxPushSize] data = void;
        
        foreach (i, ref chan; channels) chan = channel ~ "_" ~ Integer.toString(i);
                
        do synchronized (this) 
        {               
            auto rdata = getRandom(data);
            
            char[] pusher ( uint id )
            {
                return cast(char[]) rdata;
            }
            
            queue_client.pushMulti(channels, &pusher);
            
            epoll.eventLoop;        
            
            if (info.status != expected_result)
            {
                Trace.formatln("pushMulti failed,  expected status {}, got {}: {}", 
                               expected_result, info.status, info.message);
                
                throw new Exception("Unexpected result");
            }
            
            try foreach (queue; this.local_queues) 
            {
                queue.push(rdata);
            }
            catch (Exception e)
            {
                Trace.formatln("FAIL: {}", e.msg);
            }
        }
        while (--amount > 0) 
    }
        
    /***************************************************************************
    
        Pops an entry from the remote and local queue and compares the result.
        Throws if the values don't match.
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            channel         = channel to write to
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
         Returns:
             amount of popped entries
             
    ***************************************************************************/
    
    override size_t pop ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                          char[] channel, size_t amount = 1, 
                          QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {        
        queue_client.requestFinishedCallback(&this.requestFinished);
        bool error = false;
        
        do for (size_t i = 0; i < this.num_channels; ++i, error = false)
        { 
            char[] chan = channel ~ "_" ~ Integer.toString(i);
            
            ubyte[] data;
            synchronized (this)
            {
                data = this.local_queues[i].pop();
                  
                void popper ( uint, char[] value )
                {
                    if (data != cast(ubyte[]) value)
                    {
                        Trace.formatln("Error: popped value {} differs from "
                                       "pushed value {}", cast(ubyte[]) value,
                                       data);
                        
                        error = true;
                    }             
                }
                
                queue_client.pop(chan, &popper);            
                
                epoll.eventLoop;
            }
            
            if (info.status != expected_result)
            {
                Trace.formatln("pop failed, expected status {}, got {}: {}", 
                               expected_result, info.status, info.message);
                
                throw new Exception("Unexpected result");
            }                
            
            if (error) throw new Exception("Error while popping");
        }
        while (--amount > 0)
            
        return amount;
    }
    
    /***************************************************************************
    
        Consumes entries from the remote and local queue and compares the results.
        Throws if the values don't match.
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            channel         = channel to write to
            expected_result = optional, expected result code, defaults to Ok            
             
    ***************************************************************************/
        
    override void consume ( EpollSelectDispatcher epoll, 
                            QueueClient queue_client, char[] channel, 
                            QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {
        queue_client.requestFinishedCallback(&this.requestFinished);
        
        void consumer ( uint id, char[] value )
        {
            ubyte[] data;
            synchronized (this) 
            {
                data = this.local_queues[id].pop();
            
                if (data != cast(ubyte[]) value)
                {
                    Trace.formatln("Error: consumed value {} differs from "
                                   "pushed value {}", cast(ubyte[]) value,
                                   data);
                    
                    epoll.shutdown;
                }
            }
        }
        
        static char nothing;
        
        char[] chan = null;
        
        for (size_t i = 0; i < this.num_channels; ++i)
        {      
            chan = channel ~ "_" ~ Integer.toString(i);
            
            queue_client.consume(chan, 1, &consumer, null, i);
        }
        
        epoll.eventLoop;   
        
        if (info.status != expected_result)
        {
            Trace.formatln("consume failed, expected status {}, got {}: {}", 
                           expected_result, info.status, info.message);
            
            throw new Exception("Unexpected result");
        }        
    }
        
    /***************************************************************************
    
        Name of this command
        
        Returns:
            name of this command
             
    ***************************************************************************/
        
    char[] name ( )
    {
        return "pushMulti";
    }
   
    /***************************************************************************
    
        Returns:
            Whether all commands that have been pushed have been popped/consumed
             
    ***************************************************************************/
    
    bool done()
    {
        foreach (q; this.local_queues) if (q.length != 0) return false;
        
        return true;
    }
}


class PushMultiCompressed : PushMulti
{    
    /***************************************************************************
    
        Constructor
        
        Params:
            size   = size of the local test queue in bytes
    
    ***************************************************************************/
    
    this ( size_t num_channels, size_t size )
    {
        super(num_channels, size);
    }
    
    /***************************************************************************
    
        Pushes a test entry to the remote and local queue
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            channel         = channel to write to
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
    ***************************************************************************/
    
    override void push ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                         char[] channel, size_t amount = 1, 
                         QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {
        queue_client.requestFinishedCallback(&this.requestFinished);
        scope char[][] channels = new char[][num_channels];
        
        foreach (i, ref chan; channels) chan = channel ~ "_" ~ Integer.toString(i);
                
        do
        {
            int size = rand.uniformR(QueueMaxPushSize) + 1;
            
            ubyte[] data;
            synchronized (this) 
            {
                data = this.local_queues[0].push(size);
                            
                if (data is null)
                {
                    throw new Exception("Local queue is full!");
                }
                
                foreach (ref b; data) b = rand.uniformR2(ubyte.min, ubyte.max);
                
                foreach (queue; this.local_queues[1 .. $]) 
                {
                    queue.push(data);
                }
                
                char[] pusher ( uint id )
                {
                    return cast(char[]) data;
                }
                
                queue_client.pushMultiCompressed(channels, &pusher);
                
                epoll.eventLoop;
            }
            
            if (info.status != expected_result)
            {
                Trace.formatln("pushMultiCompressed failed, expected status {}, got {}: {}", 
                               expected_result, info.status, info.message);
                
                throw new Exception("Unexpected result");
            }
        }
        while (--amount > 0) 
    }
    
    /***************************************************************************
    
        Name of this command
        
        Returns:
            name of this command
             
    ***************************************************************************/
            
    char[] name ( )
    {
        return "pushMultiCompressed";
    }
}