/*******************************************************************************

    Queue Push write test

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.writeTests.WriteTests;

private import src.mod.test.writeTests.Push,
               src.mod.test.writeTests.PushMulti;

public import src.mod.test.writeTests.IWriteTest;

/*******************************************************************************

    Tango Imports

*******************************************************************************/

private import tango.util.log.Log;
 
private import Integer = tango.text.convert.Integer;

/*******************************************************************************
    
             
 
*******************************************************************************/

class WriteTests
{       
    /***************************************************************************
    
        Logger instance
    
    ***************************************************************************/
    
    private Logger logger;
       
    /***************************************************************************
    
        Channel (base) name to write to
    
    ***************************************************************************/
    
    package char[] channel;
    
    /***************************************************************************
    
        Local array for validation
    
    ***************************************************************************/
    
    package int[] items;
       
    /***************************************************************************
    
             
    ***************************************************************************/

    IWriteTest[] write_tests;
    
    /***************************************************************************
    
        Number of this instance, used for channel creation and identification
    
    ***************************************************************************/
        
    package size_t instance_number;
    
    /***************************************************************************
    
        Push counter, increased for each written entry, decreased for each
        read entry.
    
    ***************************************************************************/
    
    package size_t push_counter = 0;
    
    /***************************************************************************
    
        Maximum size of an item that will be pushed
    
    ***************************************************************************/
    
    package size_t max_item_size;
     
    /***************************************************************************
    
             
    ***************************************************************************/

    private static size_t instance_counter = 0;
    
    /***************************************************************************
    
        Constructor
        
        Params:
            size            = number of items one queue channel can hold
            item_size       = maximum amount of bytes that one item will have
            instance_number = number of this instance 
    
    ***************************************************************************/
    
    this ( size_t size, size_t item_size, size_t channels )
    {
        this.max_item_size = item_size;
        
        this.items = new int[size];
        
        this.instance_number = ++instance_counter;
        
        this.logger = Log.lookup("WriteTest["
                                 ~ Integer.toString(instance_number) ~ "]");
        
        this.channel = "test_channel_" ~ Integer.toString(instance_number);
        
        this.write_tests = 
            [cast(IWriteTest) new Push(this), 
                              new PushCompressed(this),
                              new PushMulti(this, channels),
                              new PushMultiCompressed(this, channels)
            ];
    }
    
 
    /***************************************************************************
    
        Returns:
            How many items were not consumed yet
             
    ***************************************************************************/

    public size_t itemsLeft()
    {
        return this.push_counter;
    }
    
    /***************************************************************************
    
             
    ***************************************************************************/

    public void finish()
    {
        if (this.push_counter != 0)
        {
            logger.trace("push counter: {}", push_counter);
            throw new Exception("Not all requests were processed according to"
                                " the push counter");
        }
        
        foreach (num; this.items) if (num != 0)
        {
            logger.trace("num: {}", items);
            throw new Exception("Not all requests were processed!");
        }
        
        this.push_counter = 0;
    }
        
    /***************************************************************************
    
             
    ***************************************************************************/

    int opApply ( int delegate ( ref IWriteTest test ) dg )
    {
        int result = 0;

        foreach (test; this.write_tests)
        {
            result = dg(test);

            if (result) break;
        }

        return result;
    }    
}
