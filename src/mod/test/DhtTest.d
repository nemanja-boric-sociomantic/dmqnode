/*******************************************************************************

    DHT node test

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        March 2011: Initial release

    authors:        Gavin Norman


*******************************************************************************/

module src.mod.test.DhtTest;

private import src.mod.test.tests.MemoryCommands,
               src.mod.test.tests.LogfileCommands;

private import ocean.text.Arguments;

class DhtTest
{
    private Arguments args;
    
    this ( Arguments args )
    {
        this.args = args;
    }
    
    void run ( )
    {   
        switch (this.args("type").assigned[0])
        {
            case "memory":
                (new MemoryCommands(2, this.args("source").assigned[0])).run();
                return;
            case "logfile":
                (new LogfileCommands(2, this.args("source").assigned[0])).run();
                return;
        }
    }
}