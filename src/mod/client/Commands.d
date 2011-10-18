/*******************************************************************************

    DHT command-line client commands

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        October 2011: Initial release

    authors:        Leandro Lucarella

    This module define all the available commands, and provides a framework to
    validate and show help messages about the available commands.

    The public interface is extremely simple, just use the getCommand() function
    to get a Command instance for a particular command name and then use the
    public interface of the Command class. Also you can print a help message
    with the available commands using printCommandsHelp().

    Commands are not meant to be instantiated anywhere else but in this module.
    They are singletons stored in an internal list.

*******************************************************************************/

module src.mod.client.Commands;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.dht.DhtClient,
               swarm.dht.DhtConst,
               swarm.dht.client.RequestNotification,
               swarm.dht.client.request.params.RequestParams;

private import tango.io.Stdout;
private import tango.io.stream.Format : FormatOutput;
private import tango.text.Util : join;



/*******************************************************************************

    Argument help tuple, consisting in the argument name and help message.

*******************************************************************************/

private struct ArgHelp
{
    char[] name;
    char[] help;
}


/*******************************************************************************

    Common arguments and their help messages

*******************************************************************************/

private
{
    ArgHelp help_chan    = ArgHelp("chan",  "Name of the channel to use");
    /// ditto
    ArgHelp help_value   = ArgHelp("value", "Value to put");
    /// ditto
    ArgHelp help_key     = ArgHelp("key",   "Name of the key to get");
    /// ditto
    ArgHelp help_key_min = ArgHelp("key-min",
                                   "Lower bound key from the range to get");
    /// ditto
    ArgHelp help_key_max = ArgHelp("key-max",
                                   "Upper bound key from the range to get");
}


/*******************************************************************************

    String used as a tab for help messages.

*******************************************************************************/

private const char[] TAB = "    ";


/*******************************************************************************

    Base Command class.

    It provides the common framework to define the available commands. All the
    commands derive from this class.

    Commands are created by this module are are not meant to be initialized
    anywhere else. Commands are instantiated by themselves once in theirs static
    constructor, as if they where singletons.

    All derived classes should create an instance of themselves in theirs static
    constructor, initialize all these public attributes except args and add the
    initialized instance to the list of commands using the add_command()
    function.

*******************************************************************************/

public abstract class Command
{

    /// Arguments the user passed to the command.
    protected char[][] args;

    /// Command name and aliases the user can type.
    protected char[][] command_names;

    /// Help message for this command.
    protected char[] help_msg;

    /// List of arguments this command takes and their help message.
    protected ArgHelp[] args_help;


    /***************************************************************************

        Get a list of arguments names.

        Returns:
            list of arguments names.

    ***************************************************************************/

    protected char[][] arg_names()
    {
        char[][] args;
        foreach (arg; this.args_help)
            args ~= arg.name;
        return args;
    }


    /***************************************************************************

        Validate the command arguments.

        Returns:
            null if the command arguments are valid, a string with an error
            message otherwise.

    ***************************************************************************/

    public char[] validate()
    {
        if (args.length < this.args_help.length)
            return "Too few arguments, missing argument(s): " ~
                join(this.arg_names[args.length .. $], ", ");
        if (args.length > this.args_help.length)
            return "Too many arguments, unrecognized argument(s): " ~
                join(args[this.args_help.length .. $], " ");
        //foreach (i, arg; args)
        //    if (arg.length == 0)
        //        return "argument '" ~ this.args_help[i].name ~ "' is empty";
        return null;
    }


    /***************************************************************************

        Print a help message for this command.

        Params:
            output = Where to print the message to.

    ***************************************************************************/

    public void printHelp(FormatOutput!(char) output)
    {
        output.format("Command usage: {}", this.command_names[0]);
        if (this.command_names.length > 1)
            output.format(" (or {})", join(this.command_names[1..$], ", "));
        if (this.arg_names)
            output(" ")(join(this.arg_names, " "));
        output.newline;
        output(this.help_msg).newline;
        if (this.args_help)
        {
            output.formatln("Arguments: ");
            foreach (arg; this.args_help)
                output.formatln("{}{}: {}", TAB, arg.name, arg.help);
        }
    }


    /***************************************************************************

        Assign this command as a DHT request.

        Commands *must* be validate()d before calling this method.

        Params:
            dht = DHT client to assign the request to.
            notifier = Notification callback to use.

    ***************************************************************************/

    abstract public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier);

}


/*******************************************************************************

    List of commands known by the command line client.

    Command should register themselves using the add_command() function.

*******************************************************************************/

private Command[] commands;

/// ditto
private Command[char[]] commands_by_name;


/*******************************************************************************

    Add a command to the list of commands known by the command line client.

*******************************************************************************/

private void add_command(Command c)
{
    commands ~= c;
    foreach (name; c.command_names)
    {
        debug
        {
            Command* c2 = name in commands_by_name;
            assert (c2 is null, "Command name '" ~ name ~
                "' registered by command " ~ c2.classinfo.name ~
                " already used by command " ~ c.classinfo.name);
        }
        commands_by_name[name] = c;
    }
}


/// Get command (see Command documentation for details).
private class Get : Command
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new Get;
        c.command_names = [ "get", "g" ];
        c.help_msg = "Get the associated value to a channel's key";
        c.args_help ~= help_chan;
        c.args_help ~= help_key;
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.get(this.args[0], this.args[1],
            (DhtClient.RequestContext c, char[] val) { Stdout(val).newline; },
            notifier));
    }

}

/// Put command (see Command documentation for details).
private class Put : Command
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new Put;
        c.command_names = [ "put", "p" ];
        c.help_msg = "Associate a channel's key to a value";
        c.args_help ~= help_chan;
        c.args_help ~= help_key;
        c.args_help ~= help_value;
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        // the delegate literal trick doesn't work here because it uses data
        // from the outer scope but it survives the scope of the function, so
        // the stack is used by somebody else and bad corruption happens (yei!)
        dht.assign(dht.put(this.args[0], this.args[1], &this.cb, notifier));
    }

    /// Returns the value to put (DHT client request callback)
    public char[] cb(DhtClient.RequestContext c)
    {
        return this.args[2];
    }
}

/// PutDup command (see Command documentation for details).
private class PutDup : Put
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new PutDup;
        c.command_names = [ "putdup", "pd" ];
        c.help_msg = "Associate a channel's key to a value (allowing "
                "multiple values)";
        c.args_help ~= help_chan;
        c.args_help ~= help_key;
        c.args_help ~= help_value;
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        // see Put comment, we reuse it's callback delegate.
        dht.assign(dht.putDup(this.args[0], this.args[1], &this.cb, notifier));
    }
}

/// Exists command (see Command documentation for details).
private class Exists : Command
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new Exists;
        c.command_names = [ "exists", "e" ];
        c.help_msg = "Print 1/0 if the key do/doesn't exist in the channel";
        c.args_help ~= help_chan;
        c.args_help ~= help_key;
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.exists(this.args[0], this.args[1],
            (DhtClient.RequestContext c, bool exists) {
                Stdout(exists ? 1 : 0).newline;
            },
            notifier));
    }

}

/// Exists command (see Command documentation for details).
private class Remove : Command
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new Remove;
        c.command_names = [ "remove", "r" ];
        c.help_msg = "Remove the value associated to a channel's key";
        c.args_help ~= help_chan;
        c.args_help ~= help_key;
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.remove(this.args[0], this.args[1], notifier));
    }

}

/// GetRange command (see Command documentation for details).
private class GetRange : Command
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new GetRange;
        c.command_names = [ "getrange", "gr" ];
        c.help_msg = "Get the values associated to a range of channel's keys "
                "(this probably only makes sense in combination with the "
                "--numeric-keys options)";
        c.args_help ~= help_chan;
        c.args_help ~= help_key_min;
        c.args_help ~= help_key_max;
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getRange(this.args[0], this.args[1], this.args[2],
            (DhtClient.RequestContext c, char[] key, char[] val) {
                Stdout.formatln("{}: {}", key, val);
            },
            notifier));
    }

}

/// GetAll command (see Command documentation for details).
private class GetAll : Command
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new GetAll;
        c.command_names = [ "getall", "ga" ];
        c.help_msg = "Get all the key/values present in a channel";
        c.args_help ~= help_chan;
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getAll(this.args[0],
            (DhtClient.RequestContext c, char[] key, char[] val) {
                    Stdout.formatln("{}: {}", key, val);
            },
            notifier));
    }

}

/// GetAllKeys command (see Command documentation for details).
private class GetAllKeys : Command
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new GetAllKeys;
        c.command_names = [ "getallkeys", "gak", "gk" ];
        c.help_msg = "Get all the keys present in a channel";
        c.args_help ~= help_chan;
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getAllKeys(this.args[0],
            (DhtClient.RequestContext c, char[] key) {
                Stdout(key).newline;
            },
            notifier));
    }

}

/// Listen command (see Command documentation for details).
private class Listen : Command
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new Listen;
        c.command_names = [ "listen", "l" ];
        c.help_msg = "Get all the key/values from a channel";
        c.args_help ~= help_chan;
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.listen(this.args[0],
            (DhtClient.RequestContext c, char[] key, char[] val) {
                Stdout.formatln("{}: {}", key, val);
            },
            notifier));
    }

}

/// GetChannels command (see Command documentation for details).
private class GetChannels : Command
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new GetChannels;
        c.command_names = [ "getchannels", "gc", "c" ];
        c.help_msg = "Get the names of all the channels";
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getChannels(
            (DhtClient.RequestContext c, char[] addr, ushort port,
                    char[] chan_name) {
                Stdout.formatln("{}:{} {}", addr, port, chan_name);
            },
            notifier));
    }

}

/// GetSize command (see Command documentation for details).
private class GetSize : Command
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new GetSize;
        c.command_names = [ "getsize", "gs", "s" ];
        c.help_msg = "Get the number of records and bytes for all channel "
            "on each node";
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getSize(
            (DhtClient.RequestContext c, char[] addr, ushort port,
                    ulong records, ulong bytes) {
                Stdout.formatln("{}:{} {} records, {} bytes", addr, port,
                    records, bytes);
            },
            notifier));
    }

}

/// GetChannelSize command (see Command documentation for details).
private class GetChannelSize : Command
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new GetChannelSize;
        c.command_names = [ "getchannelsize", "gcs" ];
        c.help_msg = "Get the number of records and bytes for a channel "
            "on each node";
        c.args_help ~= help_chan;
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getChannelSize(this.args[0],
            (DhtClient.RequestContext c, char[] addr, ushort port,
                    char[] chan_name, ulong records, ulong bytes) {
                Stdout.formatln("{}:{} '{}' {} records, {} bytes", addr, port,
                    chan_name, records, bytes);
            },
            notifier));
    }

}

/// RemoveChannel command (see Command documentation for details).
private class RemoveChannel : Command
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new RemoveChannel;
        c.command_names = [ "removechannel", "rc" ];
        c.help_msg = "Remove a channel and all its associated data";
        c.args_help ~= help_chan;
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.removeChannel(this.args[0], notifier));
    }

}

/// GetNumConnections command (see Command documentation for details).
private class GetNumConnections : Command
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new GetNumConnections;
        c.command_names = [ "getnumconnections", "gnc" ];
        c.help_msg = "Get the number of connections of each node";
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getNumConnections(
            (DhtClient.RequestContext c, char[] addr, ushort port, size_t n) {
                Stdout.formatln("{}:{} {} connections", addr, port, n);
            },
            notifier));
    }

}

/// GetVersion command (see Command documentation for details).
private class GetVersion : Command
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new GetVersion;
        c.command_names = [ "getversion", "gv", "v" ];
        c.help_msg = "Get the version of each node";
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getVersion(
            (DhtClient.RequestContext c, char[] addr, ushort port, char[] ver) {
                Stdout.formatln("{}:{} version {}", addr, port, ver);
            },
            notifier));
    }

}

/// GetReponsibleRange command (see Command documentation for details).
private class GetReponsibleRange : Command
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new GetReponsibleRange;
        c.command_names = [ "getreponsiblerange", "grr" ];
        c.help_msg = "Get the range of keys each node handles";
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getResponsibleRange(
            (DhtClient.RequestContext c, char[] addr, ushort port,
                    RequestParams.Range r) {
                Stdout.formatln("{}:{} {} - {}", addr, port, r.min, r.max);
            },
            notifier));
    }

}

/// GetSupportedCommands command (see Command documentation for details).
private class GetSupportedCommands : Command
{

    /// Initialize the command and add it to the internal list.
    static this()
    {
        Command c = new GetSupportedCommands;
        c.command_names = [ "getsupportedcommands", "gsc" ];
        c.help_msg = "Get the list of supported commands each node supports";
        add_command(c);
    }

    /// Assign this command as a DHT request (see Command for details).
    override public void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getSupportedCommands(
            (DhtClient.RequestContext c, char[] addr, ushort port,
                        DhtConst.Command.BaseType[] cmds) {
                foreach (cmd; cmds) {
                    auto cmd_desc = DhtConst.Command.description(cmd);
                    Stdout.formatln("{}:{} {} ({})", addr, port,
                            cmd_desc !is null ? *cmd_desc : null, cmd);
                }
            },
            notifier));
    }

}



/*******************************************************************************

    Get a command based on a command name and arguments to pass to it.

    Params:
        name = Name of the command to search for.
        args = Arguments to pass to the command.

    Returns:
        Command derived class or null if no command was found.

*******************************************************************************/

public Command getCommand(char[] name, char[][] args)
{
    Command* cmd = name in commands_by_name;
    if (cmd is null)
        return null;
    cmd.args = args.dup; // duplicate the arguments, just in case
    return *cmd;
}


/*******************************************************************************

    Print a help message for this command.

    Params:
        output = Where to print the message to.

*******************************************************************************/

public void printCommandsHelp(FormatOutput!(char) output)
{
    output.formatln("Available commands:");
    foreach (cmd; commands)
    {
        output.format("{}{}", TAB, cmd.command_names[0]);
        if (cmd.command_names.length > 1)
            output.format(" (or {})", join(cmd.command_names[1..$], ", "));
        output.formatln(": {}", cmd.help_msg);
    }
}
