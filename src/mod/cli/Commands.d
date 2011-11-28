/*******************************************************************************

    DHT command-line client commands

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        October 2011: Initial release

    authors:        Leandro Lucarella

    This module define all DHT-client specific commands, using the Command
    framework. Each command register itself in the Command registry using the
    static constructor to make maintenance easier.

*******************************************************************************/

module src.mod.cli.Commands;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.cli.Command;

private import swarm.dht.DhtClient,
               swarm.dht.DhtConst,
               swarm.dht.client.RequestNotification,
               swarm.dht.client.request.params.RequestParams;

private import ocean.io.select.EpollSelectDispatcher;

private import tango.io.Stdout;

private import Integer = tango.text.convert.Integer;



/*******************************************************************************

    Common arguments and their help messages

*******************************************************************************/

private
{
    auto help_chan       = ArgHelp("chan",  "Name of the channel to use");
    /// ditto
    auto help_value      = ArgHelp("value", "Value to put");
    /// ditto
    auto help_key        = ArgHelp("key",   "Name of the key to use");
    /// ditto
    auto help_key_more   = VarArgHelp("More keys to use");
    /// ditto
    auto help_chan_more  = VarArgHelp("More channels to use");
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

    FIXME: the final protected methods in this class are only final due to an
    unidentifiable compiler bug which caused segmentation faults upon using the
    Stdout instance inside printKey() and printValue(). Would be good to look
    into this in more depth...

*******************************************************************************/

public abstract class DhtCommand : Command
{

    /***************************************************************************

        Send requests to the DHT.

        Params:
            user_data = User data needed for the processing.

    ***************************************************************************/

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


    /***************************************************************************

        Converts a key string into a hash. The default is to interpret key
        strings as integers using tango's Integer.toLong function. This allows
        strings such as "23", "0xfff22233", etc to be handled.

        Params:
            key = key to hash

        Returns:
            hashed string

        TODO: add a command line option to Fnv hash keys, rather than integer
        converting them

    ***************************************************************************/

    final protected hash_t hash ( char[] key )
    {
        return cast(hash_t)Integer.toLong(key);
    }


    /***************************************************************************

        Displays a record key.

        Params:
            key = key to display

        TODO: add a command line option to change the key display format, like
              decimal hexa or plain ASCII.

    ***************************************************************************/

    final protected typeof(Stdout) printKey ( char[] key )
    {
        return Stdout.format("0x{:x8}", Integer.toLong(key, 16));
    }


    /***************************************************************************

        Displays a record value.

        Params:
            val = value to display

        TODO: add a command line option to change the value display format, like
              bytes in hexa, raw or ASCII escaping unprintable characters.

    ***************************************************************************/

    final protected typeof(Stdout) printValue ( char[] val )
    {
        return Stdout(val);
    }


    /***************************************************************************

        Displays a record key & value.

        Params:
            key = key to display
            val = value to display

    ***************************************************************************/

    final protected typeof(Stdout) printKeyValue ( char[] key, char[] val )
    {
        this.printKey(key)(": ");
        return printValue(val).newline;
    }


    /***************************************************************************

        Displays the address & port of a DHT node.

        Params:
            addr = address to display
            port = port to display

    ***************************************************************************/

    final protected typeof(Stdout) printAddrPort ( char[] addr, ushort port )
    {
        return Stdout.format("{}:{}", addr, port);
    }

}


/*******************************************************************************

    Get command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class Get : DhtCommand
{
    this()
    {
        super.command_names = [ "get", "g" ];
        super.help_msg = "Get the associated value to a channel's key";
        super.req_args = [ help_chan, help_key ];
        super.opt_args = [ help_key_more ];
    }

    static this()
    {
        Command.register(new Get);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        auto chan_name = super.args[0];
        foreach (i, key; super.args[1..$])
        {
            dht.assign(dht.get(chan_name, this.hash(key), &this.cb,
                    notifier).context(i + 1));
        }
    }

    private void cb ( DhtClient.RequestContext c, char[] val )
    {
        super.printKeyValue(super.args[c.integer], val);
    }
}


/*******************************************************************************

    Put command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class Put : DhtCommand
{
    this()
    {
        super.command_names = [ "put", "p" ];
        super.help_msg = "Associate a channel's key to a value";
        super.req_args = [ help_chan, help_key, help_value ];
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
        dht.assign(dht.put(super.args[0], super.hash(super.args[1]),
                &this.cb, notifier));
    }

    public char[] cb(DhtClient.RequestContext c)
    {
        return super.args[2];
    }
}


/*******************************************************************************

    PutDup command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class PutDup : Put
{
    this()
    {
        super.command_names = [ "putdup", "pd" ];
        super.help_msg = "Associate a channel's key to a value (allowing "
                "multiple values)";
        super.req_args = [ help_chan, help_key, help_value ];
    }

    static this()
    {
        Command.register(new PutDup);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        // see Put comment, we reuse its callback delegate.
        dht.assign(dht.putDup(super.args[0], super.hash(super.args[1]), &super.cb, notifier));
    }
}


/*******************************************************************************

    Exists command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class Exists : DhtCommand
{
    this()
    {
        super.command_names = [ "exists", "e" ];
        super.help_msg = "Print 1/0 if the key do/doesn't exist in the channel";
        super.req_args = [ help_chan, help_key ];
        super.opt_args = [ help_key_more ];
    }

    static this()
    {
        Command.register(new Exists);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        auto chan_name = super.args[0];
        foreach (i, key; super.args[1..$])
        {
            dht.assign(dht.exists(chan_name, this.hash(key), &this.cb,
                notifier).context(i + 1));
        }
    }

    private void cb ( DhtClient.RequestContext c, bool exists )
    {
        this.printKeyValue(this.args[c.integer], exists ? "1" : "0");
    }
}


/*******************************************************************************

    Exists command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class Remove : DhtCommand
{
    this()
    {
        super.command_names = [ "remove", "r" ];
        super.help_msg = "Remove the value associated to a channel's key";
        super.req_args = [ help_chan, help_key ];
        super.opt_args = [ help_key_more ];
    }

    static this()
    {
        Command.register(new Remove);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        auto chan_name = super.args[0];
        foreach (key; super.args[1..$])
        {
            dht.assign(dht.remove(chan_name, super.hash(key), notifier));
        }
    }
}


/*******************************************************************************

    GetRange command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class GetRange : DhtCommand
{
    this()
    {
        super.command_names = [ "getrange", "gr" ];
        super.help_msg = "Get the values associated to a range of channel's keys "
                "(this probably only makes sense in combination with the "
                "--numeric-keys options)";
        super.req_args = [
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
        dht.assign(dht.getRange(this.args[0], this.hash(this.args[1]),
                this.hash(this.args[2]), &this.cb,
            notifier));
    }

    private void cb ( DhtClient.RequestContext c, char[] key, char[] val )
    {
        this.printKeyValue(key, val);
    }
}


/*******************************************************************************

    GetAll command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class GetAll : DhtCommand
{
    this()
    {
        super.command_names = [ "getall", "ga" ];
        super.help_msg = "Get all the key/values present in a channel";
        super.req_args = [ help_chan ];
    }

    static this()
    {
        Command.register(new GetAll);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getAll(this.args[0], &this.cb, notifier));
    }

    private void cb ( DhtClient.RequestContext c, char[] key, char[] val )
    {
        this.printKeyValue(key, val);
    }
}


/*******************************************************************************

    GetAllKeys command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class GetAllKeys : DhtCommand
{
    this()
    {
        super.command_names = [ "getallkeys", "gak", "gk" ];
        super.help_msg = "Get all the keys present in a channel";
        super.req_args = [ help_chan ];
    }

    static this()
    {
        Command.register(new GetAllKeys);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getAllKeys(this.args[0], &this.cb,
            notifier));
    }
    private void cb ( DhtClient.RequestContext c, char[] key )
    {
        this.printKey(key).newline;
    }
}


/*******************************************************************************

    Listen command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class Listen : DhtCommand
{
    this()
    {
        super.command_names = [ "listen", "l" ];
        super.help_msg = "Get all the key/values from a channel";
        super.req_args = [ help_chan ];
    }

    static this()
    {
        Command.register(new Listen);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.listen(this.args[0], &this.cb, notifier));
    }

    private void cb ( DhtClient.RequestContext c, char[] key, char[] val )
    {
        this.printKeyValue(key, val);
    }
}


/*******************************************************************************

    GetChannels command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class GetChannels : DhtCommand
{
    this()
    {
        super.command_names = [ "getchannels", "gc", "c" ];
        super.help_msg = "Get the names of all the channels";
    }

    static this()
    {
        Command.register(new GetChannels);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getChannels(&this.cb, notifier));
    }

    private void cb ( DhtClient.RequestContext c, char[] addr, ushort port,
            char[] chan_name)
    {
        if ( chan_name.length ) // ignore end of list
        {
            this.printAddrPort(addr, port).formatln(" {}", chan_name);
        }
    }
}


/*******************************************************************************

    GetSize command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class GetSize : DhtCommand
{
    this()
    {
        super.command_names = [ "getsize", "gs", "s" ];
        super.help_msg = "Get the number of records and bytes for all channel "
            "on each node";
    }

    static this()
    {
        Command.register(new GetSize);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getSize(&this.cb, notifier));
    }
    private void cb ( DhtClient.RequestContext c, char[] addr, ushort port,
            ulong records, ulong bytes)
    {
        this.printAddrPort(addr, port).formatln(" {} records, {} bytes",
                records, bytes);
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
        super.command_names = [ "getchannelsize", "gcs" ];
        super.help_msg = "Get the number of records and bytes for a channel "
            "on each node";
        super.req_args = [ help_chan ];
    }

    static this()
    {
        Command.register(new GetChannelSize);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getChannelSize(this.args[0], &this.cb, notifier));
    }

    private void cb ( DhtClient.RequestContext c, char[] addr, ushort port,
            char[] chan_name, ulong records, ulong bytes )
    {
        this.printAddrPort(addr, port).formatln(" '{}' {} records, {} bytes",
                chan_name, records, bytes);
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
        super.command_names = [ "removechannel", "rc" ];
        super.help_msg = "Remove a channel and all its associated data";
        super.req_args = [ help_chan ];
        super.opt_args = [ help_chan_more ];
    }

    static this()
    {
        Command.register(new RemoveChannel);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        foreach (chan_name; super.args)
        {
            dht.assign(dht.removeChannel(chan_name, notifier));
        }
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
        super.command_names = [ "getnumconnections", "gnc" ];
        super.help_msg = "Get the number of connections of each node";
    }

    static this()
    {
        Command.register(new GetNumConnections);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getNumConnections(&this.cb, notifier));
    }

    private void cb ( DhtClient.RequestContext c, char[] addr, ushort port,
            size_t n )
    {
        this.printAddrPort(addr, port).formatln(" {} connections", n);
    }
}


/*******************************************************************************

    GetVersion command (see Command and DhtCommand documentation for details).

*******************************************************************************/

private class GetVersion : DhtCommand
{
    this()
    {
        super.command_names = [ "getversion", "gv", "v" ];
        super.help_msg = "Get the version of each node";
    }

    static this()
    {
        Command.register(new GetVersion);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getVersion(&this.cb, notifier));
    }

    private void cb ( DhtClient.RequestContext c, char[] addr, ushort port,
            char[] ver )
    {
        this.printAddrPort(addr, port).formatln(" version {}", ver);
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
        super.command_names = [ "getreponsiblerange", "grr" ];
        super.help_msg = "Get the range of keys each node handles";
    }

    static this()
    {
        Command.register(new GetReponsibleRange);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getResponsibleRange(&this.cb, notifier));
    }
    private void cb ( DhtClient.RequestContext c, char[] addr, ushort port,
            RequestParams.Range r )
    {
        this.printAddrPort(addr, port).formatln(" {} - {}", r.min, r.max);
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
        super.command_names = [ "getsupportedcommands", "gsc" ];
        super.help_msg = "Get the list of supported commands each node supports";
    }

    static this()
    {
        Command.register(new GetSupportedCommands);
    }

    protected override void assignTo(DhtClient dht,
            RequestNotification.Callback notifier)
    {
        dht.assign(dht.getSupportedCommands(&this.cb, notifier));
    }

    private void cb ( DhtClient.RequestContext c, char[] addr, ushort port,
            DhtConst.Command.BaseType[] cmds )
    {
        foreach (cmd; cmds)
        {
            auto cmd_desc = DhtConst.Command.description(cmd);
            this.printAddrPort(addr, port).formatln(" {} ({})",
                    (cmd_desc !is null) ? *cmd_desc : null, cmd);
        }
    }
}

