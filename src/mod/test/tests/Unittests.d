/*******************************************************************************

    Abstract test class

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.tests.Unittests;

/*******************************************************************************

    Internal Imports

*******************************************************************************/

private import src.mod.test.tests.Test;

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

        Abstract Test class offering functions to test many commands

*******************************************************************************/

class Unittests : Test
{    
    /***************************************************************************

        The channel that will be tested

    ***************************************************************************/

    protected char[] channel = "test_channel";

    /***************************************************************************

        Constructor
        
        Params:
            connections = amount of connections to use
            config      = path to the xml configuration file

    ***************************************************************************/

    this ( size_t connections, char[] config )
    {
        
        this.logger = Log.lookup("Unittests");
        
        super(connections, config);
    }
    
    override void run ( )
    {
        testInvalidChannelNames();
        testEmptyRecordValues();
    }
    
    protected:


    void testInvalidChannelNames ( )
    {
        logger.info("Testing invalid names");
        char[][] invalid_names = ["", "test!", "test¡", "º№*/º№", "123°", 
                                  "₀⟩₃‑ϱαιϑεν", "–ÖÄ•PB"];
                                  
        foreach ( name; invalid_names )
        {
            logger.info("\ttesting name \"{}\"", name); 
            Exception exception = null;
            
            void getter ( DhtClient.RequestContext, char[] key_str, char[] value )
            {    
                
            }
            
            with(this.dht) assign(getAll(channel, &getter, 
                                         &this.requestNotifier));        
                    
            try this.runRequest(exception);
            catch (Exception e)
            {
                exception = e;
            }
            
            if ( exception !is null )
            {
                throw new Exception("Could create channel " ~ name);
            }
        }
    }
    
    void testEmptyRecordValues ( )
    {
        
    }
}