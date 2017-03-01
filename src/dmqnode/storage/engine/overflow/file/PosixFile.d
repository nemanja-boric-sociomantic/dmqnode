/*******************************************************************************

    A thin wrapper around basic POSIX file functionality with convenience
    extensions.

    copyright: Copyright (c) 2016 sociomantic labs. All rights reserved

*******************************************************************************/

module dmqnode.storage.engine.overflow.file.PosixFile;

class PosixFile
{
    import dmqnode.storage.engine.overflow.file.FileException;

    import ocean.io.FilePath;
    import ocean.stdc.errno: EINTR, errno;
    import fcntl = ocean.stdc.posix.fcntl: open;
    import ocean.stdc.posix.fcntl: O_RDWR, O_APPEND, O_CREAT, S_IRUSR, S_IWUSR,
                                   S_IRGRP, S_IROTH;
    import ocean.stdc.posix.sys.types: off_t;
    import unistd = ocean.stdc.posix.unistd: close, unlink;
    import ocean.stdc.posix.unistd: lseek, ftruncate, fdatasync;
    import ocean.stdc.stdio: SEEK_SET;
    import ocean.util.log.Log;

    /***************************************************************************

        File name.

    ***************************************************************************/

    public char[] name;

    /***************************************************************************

        File name as NUL terminated C style string.

    ***************************************************************************/

    protected char* namec;

    /***************************************************************************

        Reusable exception.

    ***************************************************************************/

    private FileException e_;

    /***************************************************************************

        File descriptor. A negative value indicats an error opening or creating
        the file.

    ***************************************************************************/

    public int fd;

    /***************************************************************************

        Logger.

    ***************************************************************************/

    public Logger log;

    /***************************************************************************

        Counter to make the invariant fail after close() or unlink() returned.
        close() and unlink() set it to 1 upon returning. The invariant expects
        it to be at most 1. if the invariant detects it is 1, which happens when
        it is executed after close() or unlink() returned, it sets it to 2.

    ***************************************************************************/

    private uint closed = 0;

    /**************************************************************************/

    invariant ( )
    {
        assert(this.closed <= 1, "file " ~ this.name ~ " closed");
        this.closed *= 2;

        assert(this.fd >= 0, "file " ~ this.name ~ " not opened");
    }

    /***************************************************************************

        Constructor, opens or creates the file using `name` as the file name and
        `dir` as the file directory. `dir` is expected to exist.

        Params:
            dir  = the directory for the file, expected to exist
            name = the file name without directory path

        Throws:
            FileException on error creating or opening the file.

    ***************************************************************************/

    public this ( char[] dir, char[] name )
    {
        char[] fullname = FilePath.join(dir, name) ~ '\0';
        this.fd = this.open(fullname.ptr);
        this.namec = fullname.ptr;
        this.name = fullname[0 .. $ - 1];

        /*
         * Not calling this.enforce() or this.e() at this point, as doing so
         * would call the invariant, which would fail, as this.fd < 0.
         */
        if (this.fd < 0)
        {
            throw (new FileException(this.name)).useGlobalErrno("unable to open");
        }

        this.log = Log.lookup(this.name);
        this.log.info("File opened with file descriptor {}.", this.fd);
    }

    /***************************************************************************

        Opens the file or creates it if not existing. Can be overridden by a
        subclass.

        Params:
            path = the full file path as a NUL-terminated string

        Returns:
            the non-negative file descriptor on success or a negative value on
            error; on error `errno` is set appropriately.

    ***************************************************************************/

    protected int open ( char* path )
    {
        return restartInterrupted(fcntl.open(
            path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH
        ));
    }

    /***************************************************************************

        Returns:
            the size of the file.

    ***************************************************************************/

    public ulong seek ( off_t offset, int whence, char[] errmsg,
                        char[] file = __FILE__, long line = __LINE__ )
    out (pos)
    {
        assert(pos <= off_t.max);
    }
    body
    {
        offset = lseek(this.fd, offset, whence);

        this.enforce(offset >= 0, errmsg, file, line);

        return offset;
    }

    /***************************************************************************

        Truncates the file to be empty.

    ***************************************************************************/

    public void reset ( )
    {
        /*
         * Seek to the beginning because ftruncate() does not change the file
         * position.
         */
        this.seek(0, SEEK_SET, "unable to seek back when resetting");
        this.enforce(
            this.restartInterrupted(ftruncate(this.fd, 0)) >= 0,
            "unable to truncate when resetting"
        );
    }

    /***************************************************************************

        Flushes output buffers using fdatasync().

    ***************************************************************************/

    public void flush ( )
    {
        this.enforce(!fdatasync(this.fd), "flush: unable to synchronise");
    }

    /***************************************************************************

        Closes the file. Do not call any public method after this method
        returned.

    ***************************************************************************/

    public void close ( )
    out
    {
        this.closed = 1;
    }
    body
    {
        this.enforce(
            !this.restartInterrupted(unistd.close(this.fd)),
            "unable to close"
        );
        this.log.info("File closed.");
    }

    /***************************************************************************

        Closes and deletes the file. Do not call any public method after this
        method returned.

    ***************************************************************************/

    public void remove ( )
    out
    {
        this.closed = 1;
    }
    body
    {
        this.enforce(!unistd.unlink(this.namec), "unable to delete");
        this.enforce(
            !this.restartInterrupted(unistd.close(this.fd)),
            "unable to close"
        );
        this.log.info("File deleted.");
    }

    /***************************************************************************

        Throws this.e if ok is false/0/null, adding the file name, errno and
        the error message according to errno to the exception message (unless
        errno is 0).

        Params:
            ok   = condition to check
            msg  = exception message
            file = source code file where the condition is mentioned
            line = source code line where the condition is mentioned

        Throws:
            this.e (IOException) if ok is false/0/null.

    ***************************************************************************/

    public void enforce ( T ) ( T ok, char[] msg,
                                char[] file = __FILE__, long line = __LINE__ )
    {
        if (!ok)
        {
            throw this.e.useGlobalErrno(msg, file, cast(int)line);
        }
    }

    /***************************************************************************

        Returns the FileException object, creating it if needed.

        Returns:
            the FileException instance.

    ***************************************************************************/

    public FileException e ( )
    {
        if (this.e_ is null)
        {
            this.e_ = new FileException(this.name);
        }

        return this.e_;
    }

    /***************************************************************************

        Executes op, repeating if it yields a negative value and errno is EINTR,
        indicating op was interrupted by a signal.

        Params:
            op = the operation to execute, should report an error by yielding a
                 negative value and setting errno

        Returns:
            the value op yielded on its last execution.

    ***************************************************************************/

    static protected T restartInterrupted ( T ) ( lazy T op )
    {
        T x;
        errno = 0;

        do
        {
            x = op;
        }
        while (x < 0 && errno == EINTR);

        return x;
    }
}
