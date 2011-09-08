/*******************************************************************************

    Queue command tests

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.Commands;

/*******************************************************************************

    Internals Imports

*******************************************************************************/

public import src.mod.test.Exceptions;

/*******************************************************************************

    Swarm Imports

*******************************************************************************/

private import swarm.queue.QueueClient,
               swarm.queue.QueueConst,
               swarm.queue.client.request.params.RequestParams;

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

const QueuePushMultiNum = 3;
const QueueMaxPushSize = 10;


/*******************************************************************************

    News and returns an array of instances for each command of

    * push
    * pushCompressed
    * pushMulti
    * pushMultiCompressed

*******************************************************************************/

ICommand[] getCommands ( size_t size, size_t channels, size_t item_size )
{   
    static size_t instance_counter = 0;
    
    instance_counter++;
    
    return [cast(ICommand)new Push(size, item_size, instance_counter), 
            new PushCompressed(size, item_size,instance_counter),
            new PushMulti(channels, size, item_size, instance_counter),
            new PushMultiCompressed(channels, size, item_size, instance_counter)
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
    size_t instance_number;
    size_t push_counter = 0;
    size_t max_item_size;
    
    this ( size_t size, size_t item_size, size_t instance_number )
    {
        this.max_item_size = item_size;
        this.validator_array = new int[size*item_size];
        this.instance_number = instance_number;
        this.logger = Log.lookup("command." ~ this.name() ~ 
                                 "[" ~ Integer.toString(instance_number) ~ "]");
        
        this.channel = "test_channel_" ~ Integer.toString(instance_number);
    }
    
    size_t getChannelSize ( EpollSelectDispatcher epoll, QueueClient queue_client );
    
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

    size_t itemsLeft()
    {
        return this.push_counter;
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
    
    synchronized protected CommandsException validateValue 
           ( void delegate ( uint index, ubyte[] value ) success, char[] value,
             char[] file, size_t line )
    {
        ubyte[QueueMaxPushSize] data = void;
        ubyte[] gdata;
        
        if ( value.length > uint.sizeof )
        {                        
            uint num = *(cast(uint*)value.ptr);
            
            gdata = this.getRandom(data, num);
            
            if ( num >= this.validator_array.length )
            {
                // Value refers to an index that we don't have
                return new InvalidValueException(cast(ubyte[]) value,
                                                 gdata, 
                                                 this.validator_array.length,
                                                 file, line);
            }
            else if ( gdata != cast(ubyte[]) value )
            {
                // Generated data is not the same as data in the value
                return new InvalidValueException(cast(ubyte[]) value,
                                                 gdata, 
                                                 this.validator_array.length,
                                                 file, line);
            }
            else if ( this.validator_array[num] == 0 )
            {
                // Value was already read or never sent
                logger.trace("Received: {}", cast(ubyte[]) value);
                return new InconsistencyException(num, file, line);
            }
            else
            {
            	// Everything is fine
                success(num, cast(ubyte[]) value);
                
                return null;
            }
            
        }
        else if ( value.length == 0 )
        {
	        // Empty response
            return new EmptyQueueException(file, line);
        }
        else
        {
            // Value is too short
            return new InvalidValueException(cast(ubyte[]) value,
                                             null, 
                                             this.validator_array.length,
                                             file, line);
        }
    }
    
    protected void pushImpl ( QueueClient delegate ( char[], RequestParams.PutValueDg , uint ) pushFunc, 
                              EpollSelectDispatcher epoll, 
                              QueueClient queue_client, size_t amount, 
                              QueueConst.Status.BaseType expected_result,
                              size_t incr = 1)
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
            
            pushFunc(channel, &pusher, 0);
            
            epoll.eventLoop;            
            
            if (info.status != expected_result)
            {                
                throw new UnexpectedResultException(info.status, 
                                                    expected_result,
                                                    __FILE__, __LINE__);
            }       
            
            this.validator_array[push_counter++] += incr;
        }
        while (--amount > 0) 
    }    
}


class Push : ICommand
{     
    /***************************************************************************
    
        Constructor
        
        Params:
            size   = size of the local test array in bytes
    
    ***************************************************************************/
    
    this ( size_t size, size_t item_size, uint instance_counter )
    {        
        super(size, item_size, instance_counter);
    }
           
    size_t getChannelSize ( EpollSelectDispatcher epoll, QueueClient queue_client )
    {
        size_t size;
        void receiver ( uint, char[], ushort, char[], ulong records, ulong bytes )
        {
            size += bytes;
        }
        
        queue_client.getChannelSize(this.channel, &receiver);
        
        epoll.eventLoop;
        
        return size;
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
        this.pushImpl(&queue_client.push, epoll, queue_client, amount, 
                      expected_result);
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
        CommandsException exc = null;
        queue_client.requestFinishedCallback(&this.requestFinished);
        
        do
        {                  
            synchronized (this) 
            {              
                void popper ( uint, char[] value )
                {
                    exc = this.validateValue((uint num, ubyte[])
                          {
                              this.validator_array[num] --;
                              this.push_counter --;
                          }, value, __FILE__, __LINE__);
                }
                
                queue_client.pop(channel, &popper);            
                
                epoll.eventLoop;
            }
            
            if (info.status != expected_result)
            {
                throw new UnexpectedResultException(info.status, 
                                                    expected_result,
                                                    __FILE__, __LINE__);
            }
            
            if (exc !is null) throw exc;
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
        CommandsException exc = null;
        queue_client.requestFinishedCallback(&this.requestFinished);
        
        void consumer ( uint id, char[] value )
        {   
            exc = this.validateValue((uint num, ubyte[])
                  {
                      this.validator_array[num] --;
                      this.push_counter --;
                  }, value, __FILE__, __LINE__);
        }
        
        queue_client.consume(channel, 1, &consumer);
        
        epoll.eventLoop;        
                
        if (info.status != expected_result)
        {
            throw new UnexpectedResultException(info.status, 
                                                expected_result,
                                                __FILE__, __LINE__);
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
    
    this ( size_t size, size_t item_size, uint instance_counter )
    {
        super(size, item_size, instance_counter);
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
        this.pushImpl(&queue_client.pushCompressed, epoll, queue_client, amount, 
                      expected_result);
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
    protected size_t multi_responses;
    
    /***************************************************************************
    
        Amount of channels
    
    ***************************************************************************/
    
    protected size_t num_channels;
    
    /***************************************************************************
    
        Constructor
        
        Params:
            size   = size of the local test queue in bytes
    
    ***************************************************************************/
    
    this ( size_t num_channels, size_t size, size_t item_size, 
           uint instance_counter )
    {
        this.num_channels = num_channels;
        
        super(size, item_size, instance_counter);
    }
                
    /***************************************************************************
    
        
            
    ***************************************************************************/
    
    size_t getChannelSize ( EpollSelectDispatcher epoll, QueueClient queue_client )
    {
        QueueConst.Status.BaseType expected_result = QueueConst.Status.Ok;
        size_t size;
        
        void receiver ( uint, char[], ushort, char[], ulong, ulong bytes )
        {
            size += bytes;
        }

        queue_client.requestFinishedCallback(&this.requestFinished);
        
        for (size_t i = 0; i < this.num_channels; ++i)
        { 
            char[] chan = channel ~ "_" ~ Integer.toString(i);
                        
            queue_client.getChannelSize(chan, &receiver);
           
            epoll.eventLoop;

            if (info.status != expected_result)
            {
                throw new UnexpectedResultException(info.status, 
                                                    expected_result,
                                                    __FILE__, __LINE__);
            }
        }
        
        return size;
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
        foreach (i, ref chan; channels) chan = channel ~ "_" ~ Integer.toString(i);
        
        QueueClient pushFunc ( char[] channel , RequestParams.PutValueDg dg, 
                               uint context = 0)
        {
            return queue_client.pushMulti(channels, dg, context);
        }
        
        super.pushImpl(&pushFunc, epoll, queue_client, amount, expected_result, 
                       num_channels);
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
        CommandsException exc = null;
        queue_client.requestFinishedCallback(&this.requestFinished);
                
        synchronized (this) do 
        {
            for ( size_t i = 0; i < this.num_channels; ++i, exc = null )
            { 
                char[] chan = channel ~ "_" ~ Integer.toString(i);
                           
                void popper ( uint, char[] value )
                {
                    exc = this.validateValue((uint num, ubyte[])
                          {
                              this.validator_array[num] --;
                          }, value, __FILE__, __LINE__);                    
                }
                
                queue_client.pop(chan, &popper);            
                
                epoll.eventLoop;
                                
                if (info.status != expected_result)
                {
                    throw new UnexpectedResultException(info.status, 
                                                        expected_result,
                                                        __FILE__, __LINE__);
                }                
                
                if (exc !is null) throw exc;
            }
            
            this.push_counter --;
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
        CommandsException exc = null;
        uint c = 0;
        
        void consumer ( uint id, char[] value )
        {
            exc = this.validateValue((uint num, ubyte[])
            {
                this.validator_array[num] --;
                multi_responses++;
                
                if (multi_responses == num_channels)
                {
                    this.push_counter --;
                    multi_responses = 0;
                }
            }, value, __FILE__, __LINE__); 
                        
            if (exc !is null) epoll.shutdown;
        }
        
        char[] chan = null;
        
        for (size_t i = 0; i < this.num_channels; ++i)
        {      
            chan = channel ~ "_" ~ Integer.toString(i);
            
            queue_client.consume(chan, 1, &consumer, null, i);
        }
        
        try epoll.eventLoop;
        catch (Exception e) {}
        
        if (exc !is null) throw exc;
        
        if (info.status != expected_result)
        {
            throw new UnexpectedResultException(info.status, 
                                                expected_result,
                                                __FILE__, __LINE__);
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
    
    this ( size_t num_channels, size_t size, size_t item_size, uint instance_counter )
    {
        super(num_channels, size, item_size, instance_counter);
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
        foreach (i, ref chan; channels) chan = channel ~ "_" ~ Integer.toString(i);
        
        QueueClient pushFunc ( char[] channel , RequestParams.PutValueDg dg, 
                               uint context = 0)
        {
            return queue_client.pushMultiCompressed(channels, dg, context);
        }
        
        super.pushImpl(&pushFunc, epoll, queue_client, amount, expected_result, 
                       num_channels);
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