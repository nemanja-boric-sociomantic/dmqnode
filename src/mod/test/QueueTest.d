/*******************************************************************************

    Queue tester

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.QueueTest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.test.Test;

private import src.mod.test.Commands;

private import ocean.text.Arguments;

private import ocean.util.log.Trace;

/*******************************************************************************

    

*******************************************************************************/

class QueueTest
{
    static void run ( Arguments args )
    {
        foreach (opt; args("parallel").assigned) switch (opt)
        {        
            case "single":
            {
                Trace.formatln("Running single test");
                auto test = new Test(args, getCommands());
                
                break;
            }
            case "same":
                Trace.formatln("Running parallel same-channel test");
                auto cmds = getCommands();
                
                Test[5] tests;
                
                for (uint i = 0; i < 5; ++i) (tests[i] = new Test(args, cmds)).start();
                foreach (test; tests) test.join;
                
                break;            
    
            case "other":
                Trace.formatln("Running parallel other-channels test");
                Test[5] tests;
                for (uint i = 0; i < 5; ++i) (tests[i] = new Test(args, getCommands())).start();
                foreach (test; tests) test.join;
                
                break;  
        }
    }
}