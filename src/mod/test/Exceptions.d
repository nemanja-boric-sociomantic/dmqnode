/*******************************************************************************

    Queue command tests

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.Exceptions;

private import tango.core.Exception;

/*******************************************************************************

    Swarm Imports

*******************************************************************************/

private import swarm.queue.QueueClient,
               swarm.queue.QueueConst;

class CommandsException : Exception
{
    this ( char[] msg, char[] file, size_t line )
    {
        super(msg, file, line);
    }
}

class UnexpectedResultException : CommandsException
{
    QueueConst.Status.BaseType result, expected;
    
    this ( QueueConst.Status.BaseType result, 
           QueueConst.Status.BaseType expected, char[] file, size_t line )
    {
        this.result = result;
        this.expected = expected;
        
        super("Unexpected result", file, line);
    }
}


class EmptyQueueException : CommandsException
{
    this ( char[] file, size_t line )
    {
        super("Queue is empty", file, line);
    }
}

class InvalidValueException : CommandsException
{
    ubyte[] value,
            expected;
    
    size_t array_length;
    
    this ( ubyte[] value, ubyte[] expected, size_t array_len, 
           char[] file, size_t line )
    {
        this.value = value.dup;
        this.expected = expected.dup;
        
        this.array_length= array_len;
        
        super("Invalid Value", file, line);
    }
}

class InconsistencyException : CommandsException
{
    size_t index;
    
    this ( size_t index, char[] file, size_t line )
    {
        this.index = index;
        
        super("Inconsistency between sent and received data", file, line);
    }
}