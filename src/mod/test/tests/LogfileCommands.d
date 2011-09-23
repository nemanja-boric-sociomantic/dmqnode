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

private import src.mod.test.tests.Commands;

/*******************************************************************************

    Swarm Imports

*******************************************************************************/
       
private import swarm.dht.DhtClient;

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

class LogfileCommands : Commands
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
        this.testListen(&this.dht.putDup!(uint), true);
        this.testRemoveChannel();        
        this.testListen(&this.dht.putDup!(uint), false);
    }
    
    /***************************************************************************

        confirm that the local and remote state is the same

    ***************************************************************************/
    
    protected override void confirm ( ubyte[] filter = null )
    {
        this.confirmGetAll(filter);
        this.confirmGetRange(0, Iterations/3, filter);
        this.confirmGetRange(Iterations/3, Iterations/3*2, filter); 
        this.confirmGetRange(Iterations/3*2, Iterations, filter);        
        this.confirmGetAllKeys();
        this.confirmChannelSize();
        
        if ( filter !is null )
        {
            this.confirmGetAll(null);
            this.confirmGetRange(0, Iterations/3, null);
            this.confirmGetRange(Iterations/3, Iterations/3*2, null);
            this.confirmGetRange(Iterations/3*2, Iterations, null);            
        }
    }
    
    private:  
        
    /***************************************************************************

        Tests the putDup command
        
        Params:
            compress = whether to compress the commands or not

    ***************************************************************************/

    void testPutDup ( bool compress )
    {
        logger.info("Testing putDup command (writing {}k {}entries)",
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
        
        this.confirm(compress ? null : [cast(ubyte)98]);
    }   

    /***************************************************************************

        Confirm that the local and remote data is equal by using the 
        getRange command and compare the results

    ***************************************************************************/

    void confirmGetRange ( size_t start = 0, size_t end = 0, 
                           ubyte[] filter = null )
    {       
        logger.info("\tconfirming using getRange ({} - {}{})", 
                    start, end, filter.length > 0 ? ", filtered" : "");
        
        Exception exception = null;
        size_t expecting    = 0;
        ubyte[500] data     = void;
        
        void getter ( DhtClient.RequestContext, char[] key_str, char[] value )
        {    
            if ( key_str.length + value.length == 0 ) return;
                
            auto key = Integer.parse(key_str, 16);
            exception = validateValue(key, value);
            
            if ( exception !is null )
            { 
                throw exception;
            }
            else if ( filter.length > 0 )
            {
                if ( expecting == 0 ) 
                {
                    throw exception = new Exception("No more values expected");
                }
                else if ( !value.contains(cast(char[]) filter) ) 
                {
                    throw exception = new Exception("Value should be filtered");
                }
                else
                {
                    --expecting;
                }
            }
        }
              
        auto params = this.dht.getRange(channel, start, end, 
                                        &getter, &this.requestNotifier);
        
        if ( filter.length > 0 ) 
        {
            params.filter(cast(char[]) filter);

            foreach ( key; this.values ) if ( key >= start && key <= end )
            {
                auto value = this.getRandom(data, key);
                
                if ( value.contains(filter) ) expecting++;
            }
        }        

        this.dht.assign(params);
                
        this.runRequest(exception);
        
        if ( expecting != 0 ) throw new Exception("Not all expected results arrived");
    }
}