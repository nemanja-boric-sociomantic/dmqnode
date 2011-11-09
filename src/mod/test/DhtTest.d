/*******************************************************************************

    DHT node test

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        March 2011: Initial release

    authors:        Gavin Norman

*******************************************************************************/

module src.mod.test.DhtTest;

private import src.mod.test.tests.MemoryCommands,
               src.mod.test.tests.LogfileCommands,
               src.mod.test.tests.Unittests;

private import ocean.text.Arguments;

private import tango.util.log.Log;

class DhtTest
{
    private Arguments args;
    
    private Logger logger;
    
    this ( Arguments args )
    {
        this.args = args;
        
        this.logger = Log.lookup("DhtTest");
    }
    
    void run ( )
    {   
        (new Unittests(2, this.args("source").assigned[0])).run();
        
        auto repeat = this.args.getInt!(size_t)("iterations");
        
        void doTest ( ) 
        {
            switch (this.args("type").assigned[0])
            {
                case "memory":
                    (new MemoryCommands(2, this.args("source").assigned[0])).run();
                    return;
                case "logfiles":
                    (new LogfileCommands(2, this.args("source").assigned[0])).run();
                    return;
            }
        }
        
        if ( repeat == 0 ) while ( ++repeat ) 
        {
            logger.info("Running iteration {} of infinite", repeat);
            
            doTest();
        }        
        else for ( size_t i = 0; i < repeat; ++i )
        {
            logger.info("Running iteration {} of {}", i+1, repeat);
            
            doTest();
        }
    }
}