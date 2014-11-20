/*******************************************************************************

    Copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

    logrotate-friendly logger appender. The appender writes to a file which is
    expected to be rotated using the system logrotate. logrotate is expected to
    be configured to send SIGHUP to the application when the file has been
    rotated. This causes all AppendLogrotate instances to reopen their files,
    resuming writing to the original file name (as opposed to the file which has
    just been rotated and renamed).

*******************************************************************************/

module ocean.util.log.AppendLogrotate;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.io.select.client.SignalEvent;

import ocean.io.select.EpollSelectDispatcher;

import tango.core.Array : find, remove;

import tango.io.model.IFile : FileConst;
import tango.io.device.File;

import tango.util.log.Log;
import tango.util.log.AppendFile : Filer;

import tango.stdc.posix.signal : SIGHUP;



public class AppendLogrotate : Filer
{
    /***************************************************************************

        This alias for accessing static members

    ***************************************************************************/

    private alias typeof(this) This;


    /***************************************************************************

        Registry of AppendLogrotate instances plus a SIGHUP signal handler which
        reopens all registered appenders.

    ***************************************************************************/

    static private class LogReopener : SignalEvent
    {
        /***********************************************************************

            List of files which should be reopened when SIGHUP is handled.

        ***********************************************************************/

        private AppendLogrotate[] logs;


        /***********************************************************************

            Id of signal to handle.

        ***********************************************************************/

        private const reopen_signal = SIGHUP;


        /***********************************************************************

            Constructor

        ***********************************************************************/

        private this ( )
        {
            super(&this.handle, [reopen_signal]);
        }


        /***********************************************************************

            Registers the signal handler with epoll.

            Params:
                epoll = epoll selector to register with

        ***********************************************************************/

        public void register ( EpollSelectDispatcher epoll )
        {
            epoll.register(this);
        }


        /***********************************************************************

            Causes all registered files to be reopened.

        ***********************************************************************/

        public void reopenAll ( )
        {
            foreach ( log; this.logs )
            {
                log.reopen();
            }
        }


        /***********************************************************************

            Registers a file.

            Params:
                log = file to register

        ***********************************************************************/

        private void register ( AppendLogrotate log )
        out
        {
            assert(this.logs.find(log) < this.logs.length);
        }
        body
        {
            auto count = this.logs.length;
            scope ( exit ) assert(this.logs.length == count + 1);

            assert(this.logs.find(log) == this.logs.length);
            this.logs ~= log;
        }


        /***********************************************************************

            Unregisters a file.

            Params:
                log = file to unregister

        ***********************************************************************/

        private void unregister ( AppendLogrotate log )
        out
        {
            assert(this.logs.find(log) == this.logs.length);
        }
        body
        {
            auto count = this.logs.length;
            scope ( exit ) assert(this.logs.length == count - 1);

            this.logs.length = this.logs.remove(log);
        }


        /***********************************************************************

            Signal handler method. Reopens all registered files.

        ***********************************************************************/

        private void handle ( SignalInfo siginfo )
        {
            this.reopenAll();
        }
    }


    /***************************************************************************

        Global instance of LogReopener. The instance is public so that its
        signal handler event can be registered with epoll by user code.

    ***************************************************************************/

    static public LogReopener log_reopener;


    /***************************************************************************

        File style, used by constructor and reopen() method.

    ***************************************************************************/

    static private File.Style file_style;


    /***************************************************************************

        Static constructor. Instantiates the global LogReopener.

    ***************************************************************************/

    static this ( )
    {
        This.log_reopener = new LogReopener;

        This.file_style = File.WriteAppending;
        This.file_style.share = File.Share.Read;
    }


    /***************************************************************************

        Appender mask (see super class).

    ***************************************************************************/

    private Mask mask_;


    /***************************************************************************

        Path of file. Stored so that the file can be reopened at the same path
        (see reopen()).

    ***************************************************************************/

    private char[] path;


    /***************************************************************************

        Constructor. Registers this appender with the registry.

        Params:
            path = path to the logfile
            how = which layout to use

    ***************************************************************************/

    public this ( char[] path, Appender.Layout how = null )
    {
        this.path = path.dup;

        // Get a unique fingerprint for this instance
        this.mask_ = this.register(path);

        // Set the conduit (the file)
        this.configure(new File(path, This.file_style));

        // Set provided layout (ignored when null)
        this.layout(how);

        // Register with the global logrotate signal handler
        This.log_reopener.register(this);
    }


    /***************************************************************************

        Disposer. Unregisters this appender from the registry.

    ***************************************************************************/

    protected override void dispose ( )
    {
        This.log_reopener.unregister(this);
    }


    /***************************************************************************

        Returns:
            the fingerprint for this class

    ***************************************************************************/

    override public Mask mask ( )
    {
        return this.mask_;
    }


    /***************************************************************************

        Returns:
            the name of this class

    ***************************************************************************/

    override public char[] name ( )
    {
        return this.classinfo.name;
    }


    /***************************************************************************

        Appends an event to the output.

        Params:
            event = event to append

    ***************************************************************************/

    override synchronized public void append ( LogEvent event )
    {
        this.layout.format(event, &this.buffer.write);
        this.buffer.append(FileConst.NewlineString).flush;
    }


    /***************************************************************************

        Reopens the file, using the original path set in the constructor.

    ***************************************************************************/

    private void reopen ( )
    {
        (cast(File)this.conduit).open(this.path, This.file_style);
    }
}

