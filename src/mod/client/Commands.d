/*******************************************************************************

    DHT command-line client commands

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        October 2011: Initial release

    authors:        Leandro Lucarella

    This module define all DHT-client specific commands, using the Command
    framework. Each command register itself in the Command registry using the
    static constructor to make maintenance easier.

*******************************************************************************/

module src.mod.client.Commands;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.client.Command;

private import swarm.dht.DhtClient,
               swarm.dht.DhtConst,
               swarm.dht.client.RequestNotification,
               swarm.dht.client.request.params.RequestParams;

private import ocean.io.select.EpollSelectDispatcher;

private import tango.io.Stdout;



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
}


/*******************************************************************************

    DHT-client specific info to pass to commands when executing them.

*******************************************************************************/

private class Info
{
    public DhtClient dht;
    public RequestNotification.Callback notifier;
    public EpollSelectDispatcher epoll;
    this(DhtClient dht, RequestNotification.Callback notifier,
            EpollSelectDispatcher epoll)
    {
        this.dht = dht;
        this.notifier = notifier;
        this.epoll = epoll;
    }
}


/*******************************************************************************

    Base class for all DHT-client specific commands.

    This class just check the user_data passed to the execute() method and calls
    an specific method assignTo() that each subclass should implement to
    actually send the command to the DHT-nodes. Finally the eventLoop() is
    invoked.

*******************************************************************************/

public abstract class DhtCommand : Command
{

    public override void execute(Object user_data = null)
    {
        assert (user_data !is null, "user_data can't be null");
        Info info = cast(Info) user_data;
        assert (info !is null, "user_data should have Info type");
        this.assignTo(info.dht, info.notifier);
        info.epoll.eventLoop();
    }

    protected abstract void assignTo(DhtClient dht,
            RequestNotification.Callback notifier);

}


/*******************************************************************************

    Get command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class Get : DhtCommand
{
    this()
    {
        this.command_names = [ "get", "g" ];
        this.help_msg = "Get the associated value to a channel's key";
        this.req_args = [ help_chan, help_key ];
    }

    static this()
    {
        Command.register(new Get);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.get(this.args[0], this.args[1],
            (DhtClient.RequestContext c, char[] val) { Stdout(val).newline; },
            notifier));
    }
}


/*******************************************************************************

    Put command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class Put : DhtCommand
{
    this()
    {
        this.command_names = [ "put", "p" ];
        this.help_msg = "Associate a channel's key to a value";
        this.req_args = [ help_chan, help_key, help_value ];
    }

    static this()
    {
        Command.register(new Put);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        // the delegate literal trick doesn't work here because it uses data
        // from the outer scope but it survives the scope of the function, so
        // the stack is used by somebody else and bad corruption happens (yei!)
        dht.assign(dht.put(this.args[0], this.args[1], &this.cb, notifier));
    }

    public char[] cb(DhtClient.RequestContext c)
    {
        return this.args[2];
    }
}


/*******************************************************************************

    PutDup command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class PutDup : Put
{
    this()
    {
        this.command_names = [ "putdup", "pd" ];
        this.help_msg = "Associate a channel's key to a value (allowing "
                "multiple values)";
        this.req_args = [ help_chan, help_key, help_value ];
    }

    static this()
    {
        Command.register(new PutDup);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        // see Put comment, we reuse its callback delegate.
        dht.assign(dht.putDup(this.args[0], this.args[1], &this.cb, notifier));
    }
}


/*******************************************************************************

    Exists command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class Exists : DhtCommand
{
    this()
    {
        this.command_names = [ "exists", "e" ];
        this.help_msg = "Print 1/0 if the key do/doesn't exist in the channel";
        this.req_args = [ help_chan, help_key ];
    }

    static this()
    {
        Command.register(new Exists);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.exists(this.args[0], this.args[1],
            (DhtClient.RequestContext c, bool exists) {
                Stdout(exists ? 1 : 0).newline;
            },
            notifier));
    }
}


/*******************************************************************************

    Exists command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class Remove : DhtCommand
{
    this()
    {
        this.command_names = [ "remove", "r" ];
        this.help_msg = "Remove the value associated to a channel's key";
        this.req_args = [ help_chan, help_key ];
    }

    static this()
    {
        Command.register(new Remove);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.remove(this.args[0], this.args[1], notifier));
    }
}


/*******************************************************************************

    GetRange command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class GetRange : DhtCommand
{
    this()
    {
        this.command_names = [ "getrange", "gr" ];
        this.help_msg = "Get the values associated to a range of channel's keys "
                "(this probably only makes sense in combination with the "
                "--numeric-keys options)";
        this.req_args = [
            help_chan,
            ArgHelp("key-min", "Lower bound key from the range to get"),
            ArgHelp("key-max", "Upper bound key from the range to get")
        ];
    }

    static this()
    {
        Command.register(new GetRange);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getRange(this.args[0], this.args[1], this.args[2],
            (DhtClient.RequestContext c, char[] key, char[] val) {
                Stdout.formatln("{}: {}", key, val);
            },
            notifier));
    }
}


/*******************************************************************************

    GetAll command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class GetAll : DhtCommand
{
    this()
    {
        this.command_names = [ "getall", "ga" ];
        this.help_msg = "Get all the key/values present in a channel";
        this.req_args = [ help_chan ];
    }

    static this()
    {
        Command.register(new GetAll);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getAll(this.args[0],
            (DhtClient.RequestContext c, char[] key, char[] val) {
                    Stdout.formatln("{}: {}", key, val);
            },
            notifier));
    }
}


/*******************************************************************************

    GetAllKeys command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class GetAllKeys : DhtCommand
{
    this()
    {
        this.command_names = [ "getallkeys", "gak", "gk" ];
        this.help_msg = "Get all the keys present in a channel";
        this.req_args = [ help_chan ];
    }

    static this()
    {
        Command.register(new GetAllKeys);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getAllKeys(this.args[0],
            (DhtClient.RequestContext c, char[] key) {
                Stdout(key).newline;
            },
            notifier));
    }
}


/*******************************************************************************

    Listen command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class Listen : DhtCommand
{
    this()
    {
        this.command_names = [ "listen", "l" ];
        this.help_msg = "Get all the key/values from a channel";
        this.req_args = [ help_chan ];
    }

    static this()
    {
        Command.register(new Listen);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.listen(this.args[0],
            (DhtClient.RequestContext c, char[] key, char[] val) {
                Stdout.formatln("{}: {}", key, val);
            },
            notifier));
    }
}


/*******************************************************************************

    GetChannels command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class GetChannels : DhtCommand
{
    this()
    {
        this.command_names = [ "getchannels", "gc", "c" ];
        this.help_msg = "Get the names of all the channels";
    }

    static this()
    {
        Command.register(new GetChannels);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getChannels(
            (DhtClient.RequestContext c, char[] addr, ushort port,
                    char[] chan_name) {
                if (chan_name.length) // ignore end of list
                    Stdout.formatln("{}:{} '{}'", addr, port, chan_name);
            },
            notifier));
    }
}


/*******************************************************************************

    GetSize command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class GetSize : DhtCommand
{
    this()
    {
        this.command_names = [ "getsize", "gs", "s" ];
        this.help_msg = "Get the number of records and bytes for all channel "
            "on each node";
    }

    static this()
    {
        Command.register(new GetSize);
    }

    protected override void assignTo(DhtClient dht,
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


/*******************************************************************************

    GetChannelSize command (see Command and DhtCommand documentation for
    details).

*******************************************************************************/

private class GetChannelSize : DhtCommand
{
    this()
    {
        this.command_names = [ "getchannelsize", "gcs" ];
        this.help_msg = "Get the number of records and bytes for a channel "
            "on each node";
        this.req_args = [ help_chan ];
    }

    static this()
    {
        Command.register(new GetChannelSize);
    }

    protected override void assignTo(DhtClient dht,
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


/*******************************************************************************

    RemoveChannel command (see Command and DhtCommand documentation for
    details).

*******************************************************************************/

private class RemoveChannel : DhtCommand
{
    this()
    {
        this.command_names = [ "removechannel", "rc" ];
        this.help_msg = "Remove a channel and all its associated data";
        this.req_args = [ help_chan ];
    }

    static this()
    {
        Command.register(new RemoveChannel);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.removeChannel(this.args[0], notifier));
    }
}


/*******************************************************************************

    GetNumConnections command (see Command and DhtCommand documentation for
    details).

*******************************************************************************/

private class GetNumConnections : DhtCommand
{
    this()
    {
        this.command_names = [ "getnumconnections", "gnc" ];
        this.help_msg = "Get the number of connections of each node";
    }

    static this()
    {
        Command.register(new GetNumConnections);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getNumConnections(
            (DhtClient.RequestContext c, char[] addr, ushort port, size_t n) {
                Stdout.formatln("{}:{} {} connections", addr, port, n);
            },
            notifier));
    }
}


/*******************************************************************************

    GetVersion command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class GetVersion : DhtCommand
{
    this()
    {
        this.command_names = [ "getversion", "gv", "v" ];
        this.help_msg = "Get the version of each node";
    }

    static this()
    {
        Command.register(new GetVersion);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getVersion(
            (DhtClient.RequestContext c, char[] addr, ushort port, char[] ver) {
                Stdout.formatln("{}:{} version {}", addr, port, ver);
            },
            notifier));
    }
}


/*******************************************************************************

    GetReponsibleRange command (see Command and DhtCommand documentation for
    details).

*******************************************************************************/

private class GetReponsibleRange : DhtCommand
{
    this()
    {
        this.command_names = [ "getreponsiblerange", "grr" ];
        this.help_msg = "Get the range of keys each node handles";
    }

    static this()
    {
        Command.register(new GetReponsibleRange);
    }

    protected override void assignTo(DhtClient dht,
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


/*******************************************************************************

    GetSupportedCommands command (see Command and DhtCommand documentation for
    details).

*******************************************************************************/

private class GetSupportedCommands : DhtCommand
{
    this()
    {
        this.command_names = [ "getsupportedcommands", "gsc" ];
        this.help_msg = "Get the list of supported commands each node supports";
    }

    static this()
    {
        Command.register(new GetSupportedCommands);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getSupportedCommands(
            (DhtClient.RequestContext c, char[] addr, ushort port,
                        DhtConst.Command.BaseType[] cmds) {
                foreach (cmd; cmds)
                {
                    auto cmd_desc = DhtConst.Command.description(cmd);
                    Stdout.formatln("{}:{} {} ({})", addr, port,
                            cmd_desc !is null ? *cmd_desc : null, cmd);
                }
            },
            notifier));
    }
}

