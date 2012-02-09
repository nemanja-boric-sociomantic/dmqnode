/*******************************************************************************

    DHT command-line client

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        October 2011: Initial release

    authors:        Leandro Lucarella

    Send commands to the DHT nodes.

*******************************************************************************/

module src.mod.cli.DhtCli;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.cli.Command : Command;
private import src.mod.cli.Commands : Info;

private import src.mod.model.DhtTool;

private import swarm.dht.DhtClient;

private import ocean.io.select.EpollSelectDispatcher;

private import ocean.core.Array : appendCopy;

private import ocean.text.Arguments : Arguments;

private import tango.io.Stdout;


/*******************************************************************************

    Dht client tool

*******************************************************************************/

public class DhtCli : DhtTool
{
    /***************************************************************************

        Program arguments.

    ***************************************************************************/

    private char[][] arguments;


    /***************************************************************************

        Toggle verbose output.

    ***************************************************************************/

    private bool verbose;


    /***************************************************************************

        List of dht error messages which occurred during processing

    ***************************************************************************/

    private char[][] dht_errors;


    /***************************************************************************

        Returns:
            true if there were no errors during the processing

    ***************************************************************************/

    public bool succeeded ( )
    {
        return this.dht_errors.length == 0;
    }


    /***************************************************************************

        Overridden dht error callback. Stores the error message for display
        after processing.  The error messages are displayed all together at the
        end of processing so that the normal output is still readable.

        Params:
            e = dht client error info

    ***************************************************************************/

    override protected void notifier ( DhtClient.RequestNotification info )
    {
        /+
        switch (info.type) {
            case info.type.Undefined:
                Stdout("Undefined").newline;
                break;
            case info.type.Scheduled:
                Stdout("Scheduled").newline;
                break;
            case info.type.Queued:
                Stdout("Queued").newline;
                break;
            case info.type.Started:
                Stdout("Started").newline;
                break;
            case info.type.Finished:
                Stdout("Finished").newline;
                break;
            case info.type.GroupFinished:
                Stdout("GroupFinished").newline;
                break;
            default:
                Stdout("Unknown type!").newline;
        }
        +/
        if ( info.type == info.type.Finished && !info.succeeded )
        {
            super.dht_error = true;
            this.dht_errors.appendCopy(info.message);
        }
    }


    /***************************************************************************

        Main process method. Runs the tool based on the passed command line
        arguments.

    ***************************************************************************/

    protected void process_ ( )
    {
        // Show any errors in communication at exit
        scope (exit) this.displayErrors();

        auto cmd_name = this.arguments[0];
        auto args = this.arguments[1..$];
        auto cmd = Command.get(cmd_name, args);
        if (cmd is null)
        {
            Stderr.formatln("Invalid command: {}", cmd_name);
            Command.printCommandsHelp(Stderr);
            return;
        }

        char[] error = cmd.validate();
        if (error)
        {
            Stderr(error).newline;
            cmd.printHelp(Stderr);
            return;
        }

        cmd.execute(new Info(dht, &this.notifier, super.epoll));
        super.epoll.eventLoop();
    }


    /***************************************************************************

        Adds command line arguments specific to this tool.

        Params:
            args = command line arguments object to add to

    ***************************************************************************/

    override protected void addArgs_ ( Arguments args )
    {
        args("source").aliased('S').params(1).smush().required()
            .help("path of dhtnodes.xml file defining nodes to query");
        args("verbose").aliased('v')
            .help("verbose output");
    }


    /***************************************************************************

        Checks whether the parsed command line args are valid.

        Params:
            args = command line arguments object to validate

        Returns:
            true if args are valid

    ***************************************************************************/

    override protected bool validArgs ( Arguments args )
    {
        // Only allow the absense of command if --help is specified
        if ( args("help").set )
            return false;

        if ( args(null).assigned.length < 1 )
        {
            Stderr.formatln("You have to specify a command to execute");
            Stderr.newline;
            Command.printCommandsHelp(Stderr);
            return false;
        }

        return true;
    }


    /***************************************************************************

        Initialises this instance from the specified command line args.

        Params:
            args = command line arguments object to read settings from

    ***************************************************************************/

    protected void readArgs_ ( Arguments args )
    {
        super.dht_nodes_config = args.getString("source");

        this.verbose = args.getBool("verbose");

        this.arguments = args(null).assigned;
    }


    /***************************************************************************

        Returns:
            false to indicate that the tool should not fail if any errors occur
            during node handshake

    ***************************************************************************/

    override protected bool strictHandshake ( )
    {
        return false;
    }


    /***************************************************************************

        Displays any error messages which occurred during processing. The error
        messages are displayed all together at the end of processing so that
        the normal output is still readable.

    ***************************************************************************/

    private void displayErrors ( )
    {
        if ( this.dht_errors.length )
        {
            Stderr.formatln("\nDht errors which occurred during operation:");
            Stderr.formatln("------------------------------------------------------------------------------");

            foreach ( i, err; this.dht_errors )
                Stderr.formatln("  {,3}: {}", i, err);
        }
    }

}

