/*******************************************************************************

    DMQ node test runner

    Imports the DMQ test from dmqproto and runs it on the real DMQ node.

    copyright: Copyright (c) 2015 sociomantic labs. All rights reserved

*******************************************************************************/

module dmqtest.main;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import dmqtest.TestRunner;

/*******************************************************************************

    Test runner which spawns a real DMQ node to run tests on.

*******************************************************************************/

private class RealDmqTestRunner : DmqTestRunner
{
    import tango.sys.Environment;

    this ( )
    {
        super("dmqnode");
    }

    /***************************************************************************

        Copies the DMQ node's config file to the sandbox before starting the
        node.

    ***************************************************************************/

    override public CopyFileEntry[] copyFiles ( )
    {
        return [
            CopyFileEntry("test/dmqtest/etc/config.ini", "etc/config.ini"),
            CopyFileEntry("test/dmqtest/etc/credentials", "etc/credentials")
        ];
    }
}

/*******************************************************************************

    Main function. Forwards arguments to test runner.

*******************************************************************************/

int main ( istring[] args )
{
    return (new RealDmqTestRunner).main(args);
}
