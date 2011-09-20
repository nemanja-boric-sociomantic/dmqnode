/*******************************************************************************

    Memory commands test class

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.tests.LogfileCommands;

/*******************************************************************************

    Internal Imports

*******************************************************************************/

private import src.mod.test.tests.Test;

/*******************************************************************************

    Swarm Imports

*******************************************************************************/
       
private import swarm.dht.DhtClientNew;

/*******************************************************************************

    Ocean Imports

*******************************************************************************/

private import ocean.io.select.EpollSelectDispatcher,
               ocean.io.digest.Fnv1,
               ocean.util.log.SimpleLayout;

/*******************************************************************************

    Tango Imports

*******************************************************************************/

private import tango.core.Thread,
               tango.util.log.Log,
               tango.util.container.HashSet;

private import Integer = tango.text.convert.Integer;

/*******************************************************************************

    Logfile commands test class

*******************************************************************************/

class LogfileCommands : Test
{
    /***************************************************************************

        Constructor
        
        Params:
            connections = amount of connections to use
            config      = path to the xml configuration file

    ***************************************************************************/

    this ( size_t connections, char[] config )
    {
        this.logger = Log.lookup("LogfileCommands");
        
        super(connections, config);
    }

    /***************************************************************************

        Run all tests of this test class

    ***************************************************************************/

    override void run()
    {
        this.testRemoveChannel();
        this.testPutDup(true);
        this.testRemoveChannel();
        this.testPutDup(false);
        this.testRemoveChannel();
        this.testListen(&this.dht.putDup!(uint));
    }
    
    /***************************************************************************

        confirm that the local and remote state is the same

    ***************************************************************************/
    
    protected override void confirm ( )
    {
        this.confirmGetAll();
        this.confirmGetRange();
        this.confirmGetAllKeys();
        this.confirmChannelSize();
    }
    
    private:  
        
    /***************************************************************************

        Tests the putDup command
        
        Params:
            compress = whether to compress the commands or not

    ***************************************************************************/

    void testPutDup ( bool compress )
    {
        logger.info("Testing put command (writing {}k {}entries)",
                    Iterations / 1000,
                    compress ? "compressed " : "");
        Exception exception = null;
        ubyte[500] data = void;
        
        for ( size_t i = 0; i < Iterations; ++i )
        {
            char[] putter ( DhtClient.RequestContext )
            {
                return cast(char[]) this.getRandom(data, i);
            }
            
            with (this.dht) assign(putDup(channel, i, &putter, 
                                          &this.requestNotifier)
                                          .compress(compress));        
                    
            this.runRequest(exception);
            
            this.values.add(i);
        }        
        
        this.confirm();
    }   

    /***************************************************************************

        Confirm that the local and remote data is equal by using the 
        getRange command and compare the results

    ***************************************************************************/

    void confirmGetRange ( )
    {       
        logger.info("\tconfirming using getRange");
        Exception exception = null;
        
        void getter ( DhtClient.RequestContext, char[] key_str, char[] value )
        {    
            if ( key_str.length + value.length == 0 ) return;
                
            auto key = Integer.parse(key_str, 16);
            exception = validateValue(key, value);
            
            if ( exception !is null ) throw exception;
        }
        
        with(this.dht) assign(getRange(channel, cast(uint)0, this.values.size, 
                                     &getter, &this.requestNotifier));        
                
        this.runRequest(exception);
    }
}