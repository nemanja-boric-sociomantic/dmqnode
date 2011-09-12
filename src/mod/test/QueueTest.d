/*******************************************************************************

    Queue tester

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.QueueTest;

/*******************************************************************************

    Internal Imports

*******************************************************************************/

private import src.mod.test.Test,
               src.mod.test.writeTests.WriteTests;

/*******************************************************************************

    Swarm Imports

*******************************************************************************/

private import swarm.queue.QueueClient,
               swarm.queue.QueueConst;

/*******************************************************************************

    Ocean Imports

*******************************************************************************/

private import ocean.text.Arguments,
               ocean.io.select.EpollSelectDispatcher,
               ocean.util.log.Trace;

/*******************************************************************************

    Tango Imports

*******************************************************************************/

private import tango.core.sync.Barrier,
               tango.util.MinMax : max;

/*******************************************************************************

    

*******************************************************************************/

class QueueTest
{
    /***************************************************************************

        TODO: document
        TODO: check requestFinished result

     ***************************************************************************/

    static size_t getSizeLimit ( char[] config )
    {
        scope epoll  = new EpollSelectDispatcher;
                
        scope queue_client = new QueueClient(epoll, 10);
        queue_client.addNodes(config);
        
        size_t size;
        
        void receiver ( QueueClient.RequestContext, char[], ushort, ulong bytes )
        {
            size = max!(uint)(size, bytes);
        }
            
        void requestFinished ( QueueClient.RequestNotification info )
        {
            
        }   
            
        with(queue_client) assign(getSizeLimit(&receiver, &requestFinished));
        
        epoll.eventLoop;
        
        return size;
    }
    
    static void run ( Arguments args )
    {
        auto size = getSizeLimit(args("config").assigned[0]);
        auto channels = args.getInt!(size_t)("channels");
        auto items_size = args.getInt!(size_t)("size");
        
        Trace.formatln("Queue size found to be {}", size);
        
        foreach (opt; args("parallel").assigned) switch (opt)
        {        
            case "single":
            {
                Trace.formatln("Running single test");
                auto test = new Test(args, new WriteTests(size, items_size, channels));
                
                test.start;
                test.join;
                
                break;
            }
            case "same":
                Trace.formatln("Running parallel same-channel test");
                auto cmds = new WriteTests(size, items_size, channels);
                auto barrier = new Barrier(5);
                
                Test[5] tests;
                
                for (uint i = 0; i < 5; ++i) 
                {
                    (tests[i] = new Test(args, cmds, barrier)).start();
                }
                
                foreach (test; tests) test.join;
                
                break;
    
            case "other":
                Trace.formatln("Running parallel other-channels test");
                
                Test[5] tests;
                
                for (uint i = 0; i < 5; ++i) 
                {
                    (tests[i] = new Test(args, 
                                         new WriteTests(size, 
                                                        items_size, 
                                                        channels))).start();
                }
                
                foreach (test; tests) test.join;
                
                break;  
        }
    }
}