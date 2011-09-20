/*******************************************************************************

    DHT node test

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        March 2011: Initial release

    authors:        Mathias Baumann


*******************************************************************************/

module src.mod.test.tests.LogfileCommands;

/*******************************************************************************

        Notification type

*******************************************************************************/

private import src.mod.test.tests.Test;

/*******************************************************************************

        Notification type

*******************************************************************************/

private import ocean.io.select.EpollSelectDispatcher,
               ocean.io.digest.Fnv1,
               ocean.util.log.SimpleLayout;

/*******************************************************************************

        Notification type

*******************************************************************************/
       
private import swarm.dht.DhtClientNew;

/*******************************************************************************

        Notification type

*******************************************************************************/

private import tango.core.Thread,
               tango.util.log.Log,
               tango.util.container.HashSet;

private import Integer = tango.text.convert.Integer;

/*******************************************************************************

        Notification type

*******************************************************************************/

class LogfileCommands : Test
{
    this ( size_t connections, char[] config )
    {
        this.logger = Log.lookup("LogfileCommands");
        
        super(connections, config);
    }
    
    override void run()
    {
        this.testRemoveChannel();
        this.testPutDup(true);
        this.testRemoveChannel();
        this.testPutDup(false);
        this.testRemoveChannel();
        this.testListen(&this.dht.putDup!(uint));
    }
    
    protected override void confirm ( )
    {
        this.confirmGetAll();
        this.confirmGetRange();
        this.confirmGetAllKeys();
        this.confirmChannelSize();
    }
    
    private:  
        
    /***************************************************************************

        Notification type

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

        Notification type

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