/*******************************************************************************

    DHT command-line client command registry

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        October 2011: Initial release

    authors:        Leandro Lucarella

    This module defines a framework to create command-line commands accepting
    some required arguments. It also provides a framework to validate those
    arguemtns and show help messages about the available commands.

    The public interface is extremely simple, just use the Command.get()
    function to get a Command instance for a particular command name and then
    use the public interface of the Command class. Also you can print a help
    message with the available commands using Command.printCommandsHelp().

*******************************************************************************/

module src.mod.client.Command;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.dht.DhtClient,
               swarm.dht.client.RequestNotification;

private import tango.io.Stdout,
               tango.io.stream.Format : FormatOutput;

private import tango.text.Util : join, repeat;

private import tango.core.Array : map;



/*******************************************************************************

    Argument help tuple, consisting in the argument name and help message.

*******************************************************************************/

private struct ArgHelp
{
    char[] name; /// Name of the argument
    char[] help; /// Help message
}



/*******************************************************************************

    Base Command class.

    It provides the common framework to define the available commands. All the
    commands derive from this class.

    Commands are created by this module are are not meant to be initialized
    anywhere else. Commands are instantiated by themselves once in theirs static
    constructor, as if they where singletons.

    All derived classes should create an instance of themselves in theirs static
    constructor, initialize all these public attributes except args and add the
    initialized instance to the registry using the register() method.

*******************************************************************************/

public abstract class Command
{

    /***************************************************************************

        String used as a tab for help messages.

    ***************************************************************************/

    public static char[] tab = "    ";


    /***************************************************************************

        Get a command based on a command name and arguments to pass to it.

        Params:
            name = Name of the command to search for.
            args = Arguments to pass to the command.

        Returns:
            Command derived class or null if no command was found.

    ***************************************************************************/

    public static Command get(char[] name, char[][] args = null)
    {
        Command* cmd = name in this.commands_by_name;
        if (cmd is null)
        {
            return null;
        }
        cmd.args = args.dup; // duplicate the arguments, just in case
        return *cmd;
    }


    /***************************************************************************

        Print a help message for this command.

        Params:
            output = Where to print the message to.

    ***************************************************************************/

    public static void printCommandsHelp(FormatOutput!(char) output)
    {
        output.formatln("Available commands:");
        foreach (cmd; Command.commands)
        {
            output.format("{}{}", Command.tab, cmd.command_names[0]);
            if (cmd.command_names.length > 1)
            {
                output.format(" (or {})", join(cmd.command_names[1..$], ", "));
            }
            output.formatln(": {}", cmd.help_msg);
        }
    }


    /***************************************************************************

        Validate the command arguments.

        Returns:
            null if the command arguments are valid, a string with an error
            message otherwise.

    ***************************************************************************/

    public char[] validate()
    {
        auto min_args = this.req_args.length;
        if (args.length < min_args)
        {
            return "Too few arguments, missing argument(s): " ~
                join(this.req_arg_names[args.length .. $], ", ");
        }
        auto max_args = min_args + this.opt_args.length;
        if (args.length > max_args)
        {
            return "Too many arguments, unrecognized argument(s): " ~
                join(args[max_args .. $], " ");
        }
        return null;
    }


    /***************************************************************************

        Print a help message for this command.

        Params:
            output = Where to print the message to.

    ***************************************************************************/

    public void printHelp(FormatOutput!(char) output)
    {
        // Print a little header
        auto name = this.command_names[0];
        output.formatln("{}", name);
        output.formatln(repeat("-", name.length));

        // Print the help message
        output.formatln("{}.", this.help_msg);

        // Print usage
        output.format("Usage:     {}", this.command_names[0]);
        if (this.req_arg_names)
        {
            output(" ")(join(
                map(this.req_arg_names, (char[] a) {
                    return "<" ~ a ~ ">";
                }),
                " "));
        }
        if (this.opt_arg_names)
        {
            output(" ")(join(
                map(this.opt_arg_names, (char[] a) {
                    return "[" ~ a ~ "]";
                }),
                " "));
        }
        output.newline;

        // Print aliases
        if (this.command_names.length > 1)
        {
            output.formatln("Aliases:   {}",
                    join(this.command_names[1..$], ", "));
        }

        // Print arguments (if any)
        if (this.req_args || this.opt_args)
        {
            output.formatln("Arguments: ");
            foreach (arg; this.req_args)
            {
                output.formatln("{}{}: {}", Command.tab, arg.name, arg.help);
            }
            foreach (arg; this.opt_args)
            {
                output.formatln("{}{}: {}", Command.tab, arg.name, arg.help);
            }
        }
    }


    /***************************************************************************

        Assign this command as a DHT request.

        Commands *must* be validate()d before calling this method.

        Params:
            user_data = User data needed for the processing.

    ***************************************************************************/

    public abstract void execute(Object user_data = null);



    /// Arguments the user passed to the command.
    protected char[][] args;

    /// Command name and aliases the user can type.
    protected char[][] command_names;

    /// Help message for this command.
    protected char[] help_msg;

    /// List of arguments this command requires and their help message.
    protected ArgHelp[] req_args;

    /// List of optional arguments this command takes and their help message.
    protected ArgHelp[] opt_args;


    /***************************************************************************

        Get a list of required arguments names.

        Returns:
            list of arguments names.

    ***************************************************************************/

    protected char[][] req_arg_names()
    {
        char[][] args;
        foreach (arg; this.req_args)
        {
            args ~= arg.name;
        }
        return args;
    }


    /***************************************************************************

        Get a list of optional arguments names.

        Returns:
            list of arguments names.

    ***************************************************************************/

    protected char[][] opt_arg_names()
    {
        char[][] args;
        foreach (arg; this.opt_args)
        {
            args ~= arg.name;
        }
        return args;
    }


    /***************************************************************************

        Add a command to the list of commands known by the command registry.

        Params:
            cmd = Command to register in the registry.

    ***************************************************************************/

    protected static void register(Command cmd)
    {
        this.commands ~= cmd;
        foreach (name; cmd.command_names)
        {
            debug
            {
                Command* c = name in this.commands_by_name;
                assert (c is null, "Command name '" ~ name ~
                    "' registered by command " ~ c.classinfo.name ~
                    " already used by command " ~ cmd.classinfo.name);
            }
            this.commands_by_name[name] = cmd;
        }
    }


    /***************************************************************************

        List of commands known by the command line client.

        Command should register themselves using the register() method.

    ***************************************************************************/

    private static Command[] commands;

    /// ditto
    private static Command[char[]] commands_by_name;

}


/*******************************************************************************

    Command to get help about other commands (see Command for details).

*******************************************************************************/

private class Help : Command
{
    this()
    {
        this.command_names = [ "help", "h" ];
        this.help_msg = "Get general help on the available commands, or "
                "detailed help about the commands passed as arguments, if any";
        this.opt_args = [
                ArgHelp("cmd", "Get detailed help about this command"),
                ArgHelp("...", "Get detailed help about more commands")];
    }

    static this()
    {
        Command.register(new Help);
    }

    override public char[] validate()
    {
        // Search for unknown commands
        char[][] unknown;
        foreach (arg; this.args)
        {
            auto cmd = Command.get(arg, this.args);
            if (cmd is null)
            {
                unknown ~= arg;
            }
        }

        // If any, return an error with the list of unknow commands
        if (unknown)
        {
            char[] s;
            if (unknown.length > 1)
            {
                s = "s";
            }
            return "Unknown command" ~ s ~ ": " ~ join(unknown, ", ");
        }

        return null;
    }

    override public void execute(Object user_data = null)
    {
        Stdout.formatln("DHT command line client");
        Stdout.formatln("=======================").newline;

        // No arguments, show list of available commands
        if (this.args.length == 0)
        {
            Command.printCommandsHelp(Stdout);
            return;
        }

        // Show detailed help on each command passed as argument
        Command.get(this.args[0], this.args).printHelp(Stdout);
        foreach (arg; this.args[1..$])
        {
            Stdout.newline;
            Command.get(arg, this.args).printHelp(Stdout);
        }
    }

}

