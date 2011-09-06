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
               ocean.util.log.Trace,
               ocean.io.digest.Fnv1;

/*******************************************************************************

    Tango Imports

*******************************************************************************/

private import tango.math.random.Random,
               tango.util.log.Log;

private import Integer = tango.text.convert.Integer;

/*******************************************************************************

    Constants
    
    TODO: move them to cmd line parameters

*******************************************************************************/

const QueueSize = 10 * 1024 * 1024;  // 10mb
const QueuePushMultiNum = 3;
const QueueMaxPushSize = 10;


/*******************************************************************************

    News and returns an array of instances for each command of

    * push
    * pushCompressed
    * pushMulti
    * pushMultiCompressed

*******************************************************************************/

ICommand[] getCommands ( )
{   
    static size_t instance_counter = 0;
    
    instance_counter++;
    
    return [cast(ICommand)new Push(QueueSize, instance_counter), 
            //new PushCompressed(QueueSize),
            new PushMulti(QueuePushMultiNum, QueueSize, instance_counter)
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
    Logger logger;
    
    char[] channel;
        
    /***************************************************************************
    
        Local array for validation
    
    ***************************************************************************/
    
    protected int[] validator_array;
    
    size_t push_counter = 0;
    
    this ( size_t size, size_t instance_number )
    {   
        this.validator_array = new int[size];
        
        this.logger = Log.lookup("command." ~ this.name() ~ 
                                 "[" ~ Integer.toString(instance_number) ~ "]");
        
        this.channel = "test_channel_" ~ Integer.toString(instance_number);
    }
    
    /***************************************************************************
    
        Pushes a test entry to the remote and local queue
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
    ***************************************************************************/
    
    void push ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                size_t amount = 1, 
                QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok );
        
    /***************************************************************************
    
        Pops an entry from the remote and local queue and compares the result.
        Throws if the values don't match.
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
         Returns:
             amount of popped entries
             
    ***************************************************************************/
    
    size_t pop ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                 size_t amount = 1, 
                 QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok );
        
    /***************************************************************************
    
        Consumes entries from the remote and local queue and compares the results.
        Throws if the values don't match.
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            expected_result = optional, expected result code, defaults to Ok            
             
    ***************************************************************************/
    
    void consume ( EpollSelectDispatcher epoll, QueueClient queue_client, 
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

    bool done()
    {
        return this.push_counter == 0;
    }
        
    void finish()
    {
        if (this.push_counter != 0)
        {
            logger.trace("push counter: {}", push_counter);
            throw new Exception("Not all requests were processed according to"
                                " the push counter");
        }
        
        foreach (num; this.validator_array) if (num != 0)
        {
            logger.trace("num: {}", validator_array);
            throw new Exception("Not all requests were processed!");
        }
        
        this.push_counter = 0;
    }
    
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
    
    protected ubyte[] getRandom ( ubyte[] data, uint init )
    {
        uint i = Fnv1(init) % (QueueMaxPushSize-uint.sizeof) + 1;
 
        data[0 .. uint.sizeof] = (cast(ubyte*)&init) [0 .. uint.sizeof];
        
        foreach (ref b; data[uint.sizeof .. i + uint.sizeof]) 
        {
            b = Fnv1(++init) ;
        }
       
        return data[0 .. i + uint.sizeof];
    }
}


class Push : ICommand
{     
    /***************************************************************************
    
        Constructor
        
        Params:
            size   = size of the local test array in bytes
    
    ***************************************************************************/
    
    this ( size_t size, uint instance_counter )
    {        
        super(size, instance_counter);
    }
        
    /***************************************************************************
    
        Pushes a test entry to the remote and local queue
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
    ***************************************************************************/
    
    override void push ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                         size_t amount = 1, 
                         QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {        
        queue_client.requestFinishedCallback(&this.requestFinished);
        ubyte[QueueMaxPushSize] data = void;
        
        do synchronized (this)
        {             
            auto rdata = getRandom(data, this.push_counter);
            
            char[] pusher ( uint id )
            {
                return cast(char[]) rdata;
            }
            
            queue_client.push(channel, &pusher);
            
            epoll.eventLoop;            
            
            if (info.status != expected_result)
            {
                logger.error("push failed, expected status {}, got {}: {}", 
                              expected_result, info.status, info.message);
                
                throw new Exception("Unexpected result");
            }       
            
            this.validator_array[push_counter++] ++;
        }
        while (--amount > 0) 
    }
        
    /***************************************************************************
    
        Pops an entry from the remote and local queue and compares the result.
        Throws if the values don't match.
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
         Returns:
             amount of popped entries
             
    ***************************************************************************/
    
    override size_t pop ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                          size_t amount = 1, 
                          QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {
        bool error = false;
        queue_client.requestFinishedCallback(&this.requestFinished);
        ubyte[QueueMaxPushSize] data = void;
        
        do
        {                  
            synchronized (this) 
            {                
                void popper ( uint, char[] value )
                {
                    ubyte[] gdata;
                    
                    if (value.length > uint.sizeof )
                    {                        
                        uint num = *(cast(uint*)value.ptr);
                        
                        gdata = this.getRandom(data, num);
                        
                        if ( num >= this.validator_array.length )
                        {
                            error = true;   
                            logger.error("Popped value {} is invalid ({})", 
                                         cast(ubyte[]) value, this.validator_array.length);
                        }
                        else if ( gdata != cast(ubyte[]) value )
                        {
                            error = true;   
                            logger.error("Popped value {} differs from expected"
                                         " value {}", cast(ubyte[]) value, gdata);
                        }
                        else if ( this.validator_array[num] == 0 )
                        {
                            error = true;   
                            logger.error("Popped value {} was already popped earlier or never pushed",
                                         cast(ubyte[]) value);
                        }
                        else
                        {
                            this.validator_array[num] --;
                            this.push_counter --;
                        }
                        
                        return;
                    }
                    
                    logger.error("Popped value {} is too short", 
                                 cast(ubyte[]) value);  
                    error = true;    
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
            expected_result = optional, expected result code, defaults to Ok            
             
    ***************************************************************************/
    
    override void consume ( EpollSelectDispatcher epoll, 
                            QueueClient queue_client,
                            QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {
        queue_client.requestFinishedCallback(&this.requestFinished);
        ubyte[QueueMaxPushSize] data = void;
        
        void consumer ( uint id, char[] value )
        {
            synchronized (this) 
            {
                ubyte[] gdata;
                
                if (value.length > uint.sizeof )
                {                        
                    uint num = *(cast(uint*)value.ptr);
                    
                    gdata = this.getRandom(data, num);
                    
                    if ( num >= this.validator_array.length )
                    {
                        epoll.shutdown;
                        logger.error("Consumed value {} is invalid ({})", 
                                     cast(ubyte[]) value, this.validator_array.length);
                    }
                    else if ( gdata != cast(ubyte[]) value )
                    {
                        epoll.shutdown;
                        logger.error("Consumed value {} differs from expected"
                                     " value {}", cast(ubyte[]) value, gdata);
                    }
                    else if ( this.validator_array[num] == 0 )
                    {
                        epoll.shutdown;
                        logger.error("Consumed value {} was already popped earlier or never pushed",
                                     cast(ubyte[]) value);
                    }
                    else
                    {
                        this.validator_array[num] --;
                    }
                    
                    return;
                }
                
                logger.error("Consumed value {} is too short", 
                             cast(ubyte[]) value);            

                epoll.shutdown;
            }
        }
        
        queue_client.consume(channel, 1, &consumer);
        
        epoll.eventLoop;        
                
        if (info.status != expected_result)
        {
            logger.error("consume failed, expected status {}, got {}: {}", 
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
}

class PushCompressed : Push
{     
           
    /***************************************************************************
    
        Constructor
        
        Params:
            size   = size of the local test queue in bytes
    
    ***************************************************************************/
    
    this ( size_t size, uint instance_counter )
    {
        super(size, instance_counter);
    }  
    
    /***************************************************************************
    
        Pushes a test entry to the remote and local queue
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
    ***************************************************************************/
    
    override void push ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                         size_t amount = 1, 
                         QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {
        queue_client.requestFinishedCallback(&this.requestFinished);
        ubyte[QueueMaxPushSize] data = void;
        
        do synchronized (this) 
        {               
            auto rdata = getRandom(data, push_counter++);
             
            char[] pusher ( uint id )
            {
                return cast(char[]) rdata;
            }
            
            queue_client.pushCompressed(channel, &pusher);
            
            epoll.eventLoop;        
            
            if (info.status != expected_result)
            {
                logger.error("pushCompressed failed, expected status {}, got {}: {}", 
                               expected_result, info.status, info.message);
                
                throw new Exception("Unexpected result");
            }
                        
            this.validator_array[push_counter] = true;
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
    
        Amount of channels
    
    ***************************************************************************/
    
    protected size_t num_channels;
    
    /***************************************************************************
    
        Constructor
        
        Params:
            size   = size of the local test queue in bytes
    
    ***************************************************************************/
    
    this ( size_t num_channels, size_t size, uint instance_counter )
    {
        this.num_channels = num_channels;
        
        super(size, instance_counter);
    }
        
    /***************************************************************************
    
        Pushes a test entry to the remote and local queue
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
    ***************************************************************************/
    
    override void push ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                         size_t amount = 1, 
                         QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {
        scope char[][] channels = new char[][num_channels];
        queue_client.requestFinishedCallback(&this.requestFinished);
        ubyte[QueueMaxPushSize] data = void;
        
        foreach (i, ref chan; channels) chan = channel ~ "_" ~ Integer.toString(i);
                
        do synchronized (this) 
        {               
            auto rdata = getRandom(data, push_counter);
            
            char[] pusher ( uint id )
            {
                return cast(char[]) rdata;
            }
            
            queue_client.pushMulti(channels, &pusher);
            
            epoll.eventLoop;        
            
            if (info.status != expected_result)
            {
                logger.error("pushMulti failed,  expected status {}, got {}: {}", 
                               expected_result, info.status, info.message);
                
                throw new Exception("Unexpected result");
            }

            logger.trace("counter: {}", push_counter);
            this.validator_array[push_counter++] += num_channels;
        }
        while (--amount > 0) 
    }
        
    /***************************************************************************
    
        Pops an entry from the remote and local queue and compares the result.
        Throws if the values don't match.
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
         Returns:
             amount of popped entries
             
    ***************************************************************************/
    
    override size_t pop ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                          size_t amount = 1, 
                          QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {        
        queue_client.requestFinishedCallback(&this.requestFinished);
        bool error = false;
        ubyte[QueueMaxPushSize] data = void;
        
        synchronized (this)
        {
            do 
            {
                for (size_t i = 0; i < this.num_channels; ++i, error = false)
                { 
                    char[] chan = channel ~ "_" ~ Integer.toString(i);
                               
                    void popper ( uint, char[] value )
                    {
                        ubyte[] gdata;
                        if (value.length > uint.sizeof )
                        {                        
                            uint num = *(cast(uint*)value.ptr);
                            
                            gdata = this.getRandom(data, num);
                            
                            if ( num >= this.validator_array.length )
                            {
                                error = true;
                                logger.error("Popped value {} is invalid ({})", 
                                             cast(ubyte[]) value, this.validator_array.length);
                            }
                            else if ( gdata != cast(ubyte[]) value )
                            {
                                error = true;
                                logger.error("Popped value {} differs from expected"
                                             " value {}", cast(ubyte[]) value, gdata);
                            }
                            else if ( this.validator_array[num] == 0 )
                            {
                                error = true;
                                logger.error("Popped value {} was already popped earlier or never pushed",
                                             cast(ubyte[]) value);
                            }
                            else
                            {
                                logger.trace("Popped({}) {}", num, cast(ubyte[])value);
                                this.validator_array[num] --;
                            }
                            
                            return;
                        }
                        
                        error = true;   
                        
                        logger.error("Popped value {} is too short", 
                                     cast(ubyte[]) value);                       
                    }
                    
                    queue_client.pop(chan, &popper);            
                    
                    epoll.eventLoop;
                                    
                    if (info.status != expected_result)
                    {
                        logger.error("pop failed, expected status {}, got {}: {}", 
                                       expected_result, info.status, info.message);
                        
                        throw new Exception("Unexpected result");
                    }                
                    
                    if (error) throw new Exception("Error while popping");
                }
                
                this.push_counter --;
            }
            while (--amount > 0)
        }
        return amount;
    }
    
    /***************************************************************************
    
        Consumes entries from the remote and local queue and compares the results.
        Throws if the values don't match.
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            expected_result = optional, expected result code, defaults to Ok            
             
    ***************************************************************************/
        
    override void consume ( EpollSelectDispatcher epoll, 
                            QueueClient queue_client,
                            QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {
        queue_client.requestFinishedCallback(&this.requestFinished);
        ubyte[QueueMaxPushSize] data = void;
        uint c = 0;
        
        void consumer ( uint id, char[] value )
        {
            synchronized (this) 
            {
                ubyte[] gdata;
                
                if (value.length > uint.sizeof )
                {                        
                    uint num = *(cast(uint*)value.ptr);
                    
                    gdata = this.getRandom(data, num);
                    
                    if ( num >= this.validator_array.length )
                    {
                        epoll.shutdown;
                        logger.error("Consumed value {} is invalid ({})", 
                                     cast(ubyte[]) value, this.validator_array.length);
                    }
                    else if ( gdata != cast(ubyte[]) value )
                    {
                        epoll.shutdown;
                        logger.error("Consumed value {} differs from expected"
                                     " value {}", cast(ubyte[]) value, gdata);
                    }
                    else if ( this.validator_array[num] == 0 )
                    {
                        epoll.shutdown;
                        logger.error("Consumed value {} was already consumed earlier or never pushed",
                                     cast(ubyte[]) value);
                    }
                    else
                    {
                        this.validator_array[num] --;
                        c++;
                        
                        if (c == num_channels)
                        {
                            this.push_counter --;
                            c = 0;
                        }
                    }
                    
                    return;
                }
                
                logger.error("Consumed value {} is too short", 
                             cast(ubyte[]) value);            

                epoll.shutdown;
            }
        }
        
        char[] chan = null;
        
        for (size_t i = 0; i < this.num_channels; ++i)
        {      
            chan = channel ~ "_" ~ Integer.toString(i);
            
            queue_client.consume(chan, 1, &consumer, null, i);
        }
        
        epoll.eventLoop;   
        
        if (info.status != expected_result)
        {
            logger.error("consume failed, expected status {}, got {}: {}", 
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
}


class PushMultiCompressed : PushMulti
{    
    /***************************************************************************
    
        Constructor
        
        Params:
            size   = size of the local test queue in bytes
    
    ***************************************************************************/
    
    this ( size_t num_channels, size_t size, uint instance_counter )
    {
        super(num_channels, size, instance_counter);
    }
    
    /***************************************************************************
    
        Pushes a test entry to the remote and local queue
        
        Params:
            epoll           = epoll select dispatcher instance
            queue_client    = queue client instance
            amount          = optional, how many pushes to execute
            expected_result = optional, expected result code, defaults to Ok
            
    ***************************************************************************/
    
    override void push ( EpollSelectDispatcher epoll, QueueClient queue_client, 
                         size_t amount = 1, 
                         QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok )
    {
        scope char[][] channels = new char[][num_channels];
        queue_client.requestFinishedCallback(&this.requestFinished);
        ubyte[QueueMaxPushSize] data = void;
        
        foreach (i, ref chan; channels) chan = channel ~ "_" ~ Integer.toString(i);
                
        do synchronized (this) 
        {               
            auto rdata = getRandom(data, push_counter);
            
            char[] pusher ( uint id )
            {
                return cast(char[]) rdata;
            }
            
            queue_client.pushMultiCompressed(channels, &pusher);
            
            epoll.eventLoop;        
            
            if (info.status != expected_result)
            {
                logger.error("pushMultiCompressed failed,  expected status {}, got {}: {}", 
                               expected_result, info.status, info.message);
                
                throw new Exception("Unexpected result");
            }
            
            this.push_counter += num_channels;
            
            push_counter++;
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