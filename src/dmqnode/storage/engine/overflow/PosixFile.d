/*******************************************************************************

    Copyright (c) 2015 sociomantic labs. All rights reserved

    A thin wrapper around POSIX file I/O functionality with convenience
    extensions.

*******************************************************************************/

module dmqnode.storage.engine.overflow.PosixFile;

import ocean.core.ErrnoIOException;

class PosixFile
{
    import tango.stdc.posix.fcntl: open, O_RDWR, O_APPEND, O_CREAT, S_IRUSR, S_IWUSR, S_IRGRP, S_IROTH;
    import tango.stdc.posix.unistd: write, pwrite, lseek, ftruncate, fdatasync;
    import unistd = tango.stdc.posix.unistd: close, unlink;
    import tango.stdc.posix.sys.uio: writev;
    import tango.stdc.posix.sys.types: off_t, ssize_t;
    import tango.stdc.stdio: SEEK_SET;
    import tango.stdc.errno: EINTR, errno;

    import tango.io.FilePath;

    import tango.util.log.Log;

    /***************************************************************************

        File name.

    ***************************************************************************/

    public const char[] name;

    /***************************************************************************

        Logger.

    ***************************************************************************/

    public const Logger log;

    /***************************************************************************

        File name as NUL terminated C style string.

    ***************************************************************************/

    protected const char* namec;

    /***************************************************************************

        Reusable exception.

    ***************************************************************************/

    protected const FileException e;

    /***************************************************************************

        File descriptor. A negative value indicats an error opening or creating
        the file.

    ***************************************************************************/

    public const int fd;

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

        Constructor.

        Params:
            dir  = working directory
            name = file name

        Throws:
            FileException on file I/O error.

    ***************************************************************************/

    public this ( char[] dir, char[] name )
    {
        this.log = Log.lookup(name);

        this.name = FilePath.join(dir, name) ~ '\0';
        this.namec = this.name.ptr;
        this.name = this.name[0 .. $ - 1];

        this.e = new FileException;
        this.e.filename = this.name;

        this.fd = this.restartInterrupted(open(this.namec, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH));
        this.e.enforce(this.fd >= 0, "unable to open");

        this.log.info("File opened with file descriptor {}.", this.fd);
    }

    /***************************************************************************

        Returns:
            the size of the file.

    ***************************************************************************/

    public ulong seek ( off_t offset, int whence, char[] errmsg, char[] file = __FILE__, long line = __LINE__ )
    out (pos)
    {
        assert(pos <= off_t.max);
    }
    body
    {
        offset = lseek(this.fd, offset, whence);

        this.e.enforce(offset >= 0, errmsg, file, line);

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
        this.e.enforce(this.restartInterrupted(ftruncate(this.fd, 0)) >= 0, "unable to truncate when resetting");
    }

    /***************************************************************************

        Flushes output buffers using fdatasync().

    ***************************************************************************/

    public void flush ( )
    {
        this.e.enforce(!fdatasync(this.fd), "flush: unable to synchronise");
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
        this.e.enforce(!this.restartInterrupted(unistd.close(this.fd)), "unable to close");
        this.log.info("File closed.");
    }

    /***************************************************************************

        Deletes the file. Do not call any public method after this method
        returned.

    ***************************************************************************/

    public void unlink ( )
    out
    {
        this.closed = 1;
    }
    body
    {
        this.e.enforce(!unistd.unlink(this.namec), "unable to delete");
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
            file = source code line where the condition is mentioned

        Throws:
            this.e (IOException) if ok is false/0/null.

    ***************************************************************************/

    public void enforce ( T ) ( T ok, char[] msg, char[] file = __FILE__, long line = __LINE__ )
    {
        this.e.enforce(ok, msg, file, line);
    }

    /***************************************************************************

        Reads or writes data from/to the file starting at position pos. Invokes
        op to perform the I/O operation.
        op may not transmit all data with each call and should return the number
        of bytes transmitted or a negative value on error. op is repeatedly
        called until
         - all bytes in data were transmitted or
         - op returned 0; the number of remaining bytes is then returned, or
         - op returned a negative value and set errno a value different to
           EINTR; a FileException is then thrown.

        pos is increased by the number of bytes written, which is data.length -
        the returned value.

        Params:
            data = source or destination buffer to read from or write to, resp.
            pos  = file position, increased by the number of bytes read/written
            op   = I/O function
            errmsg = error message to use if op returns -1
            line = source code line of the call of this method

        Returns:
            the number of bytes n in data that have not been transmitted because
            op returned 0. The remaining bytes are data[$ - n .. $] so n == 0
            indicates that all bytes have been transmitted.

        Throws:
            FileException if op returns a negative value and sets errno to a
            value different to EINTR.

    ***************************************************************************/

    public size_t transmit ( void[] data, ref off_t pos, typeof(&pwrite) op, char[] errmsg,
                             char[] file = __FILE__, long line = __LINE__ )
    in
    {
        assert(pos >= 0);
    }
    out (n)
    {
        assert(n <= data.length);
    }
    body
    {
        for (void[] left = data; left.length;)
        {
            if (ssize_t n = this.restartInterrupted(op(this.fd, data.ptr, data.length, pos)))
            {
                if (n > 0)
                {
                    left = left[n .. $];
                    pos += n;
                }
                else
                {
                    throw this.e(errmsg, __FILE__, line);
                }
            }
            else // end of file for pread(); pwrite() should
            {    // return 0 iff data.length is 0
                return left.length;
            }
        }

        return 0;
    }

    /***************************************************************************

        Reads or writes data from/to the file at the current position. Invokes
        op to perform the I/O operation.
        op may not transmit all data with each call and should return the number
        of bytes transmitted or a negative value on error. op is repeatedly
        called until
         - all bytes in data were transmitted or
         - op returned 0; the number of remaining bytes is then returned, or
         - op returned a negative value and set errno a value different to
           EINTR; a FileException is then thrown.

        Params:
            data = source or destination buffer to read from or write to, resp.
            op   = I/O function
            errmsg = error message to use if op returns -1
            line = source code line of the call of this method

        Returns:
            the number of bytes n in data that have not been transmitted because
            op returned 0. The remaining bytes are data[$ - n .. $] so n == 0
            indicates that all bytes have been transmitted.

        Throws:
            FileException if op returns a negative value and sets errno to a
            value different to EINTR.

    ***************************************************************************/

    public size_t transmit ( void[] data, typeof(&write) op, char[] errmsg,
                             char[] file = __FILE__, long line = __LINE__ )
    out (n)
    {
        assert(n <= data.length);
    }
    body
    {
        for (void[] left = data; left.length;)
        {
            if (ssize_t n = this.restartInterrupted(op(this.fd, left.ptr, left.length)))
            {
                if (n > 0)
                {
                    left = left[n .. $];
                }
                else
                {
                    throw this.e(errmsg, __FILE__, line);
                }
            }
            else // end of file for read(); write() should
            {    // return 0 iff data.length is 0
                return left.length;
            }
        }

        return 0;
    }

    /***************************************************************************

        Reads or writes data from/to the file at the current position. Invokes
        op to perform the I/O operation.
        op may not transmit all data with each call and should return the number
        of bytes transmitted or a negative value on error. op is repeatedly
        called until
         - all bytes in data were transmitted or
         - op returned 0; the number of remaining bytes is then returned, or
         - op returned a negative value and set errno a value different to
           EINTR; a FileException is then thrown.

        Params:
            data = vector of source or destination buffers to read from or write
                   to, resp.
            op   = I/O function
            errmsg = error message to use if op returns -1
            line = source code line of the call of this method

        Returns:
            the number of bytes n in data that have not been transmitted because
            op returned 0. n == 0 indicates that all bytes have been
            transmitted. data is adjusted to reference only the remaining
            chunks.

        Throws:
            FileException if op returns a negative value and sets errno to a
            value different to EINTR.

    ***************************************************************************/

    public size_t transmit ( ref IoVec data, typeof(&writev) op, char[] errmsg,
                             char[] file = __FILE__, long line = __LINE__ )
    {
        while (data.length)
        {
            if (ssize_t n = this.restartInterrupted(op(this.fd, data.chunks.ptr, data.chunks.length)))
            {
                if (n > 0)
                {
                    data.advance(n);
                }
                else
                {
                    throw this.e(errmsg, __FILE__, line);
                }
            }
            else // end of file for read(); write() should
            {    // return 0 iff data.length is 0
                return data.length;
            }
        }

        return 0;
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

    private static T restartInterrupted ( T ) ( lazy T op )
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

/******************************************************************************/

class FileException: ErrnoIOException
{
    import ocean.core.Exception: enforce, enforceImpl;

    public char[] filename = null;

    public void enforce ( T ) ( T ok, char[] msg, char[] file = __FILE__, long line = __LINE__ )
    {
        enforceImpl(this.opCall(msg), ok, msg.init, file, line);
    }

    override public typeof (this) opCall ( char[] msg, char[] file = __FILE__, long line = __LINE__ )
    {
        char[][3] namemsg;
        namemsg[0] = this.filename;
        namemsg[1] = ": ";
        namemsg[2] = msg;
        return cast(typeof(this))super.opCall(namemsg, file, line);
    }
}

/*******************************************************************************

    Vector aka. scatter/gather I/O helper; tracks the byte position if
    readv()/writev() didn't manage to transfer all data with one call.

*******************************************************************************/

struct IoVec
{
    import tango.stdc.posix.sys.uio: writev, iovec;
    import tango.core.Exception: onArrayBoundsError;

    /***************************************************************************

        The vector of buffers. Pass to this.chunks.ptr and this.chunks.length to
        readv()/writev().

    ***************************************************************************/

    iovec[] chunks;

    /***************************************************************************

        The remaining number of bytes to transfer.

    ***************************************************************************/

    size_t length;

    /***************************************************************************

        Adjusts this.chunks and this.length after n bytes have been transferred
        by readv()/writev() so that this.chunks.ptr and this.chunks.length can
        be passed to the next call.

        Resets this instance if n == this.length, i.e. all data have been
        transferred at once. Does nothing if n is 0.

        Params:
            n = the number of bytes that have been transferred according to the
                return value of readv()/writev()

        Returns:
            the number of bytes remaining ( = this.length).

        In:
            n must be at most this.length.

    ***************************************************************************/

    size_t advance ( size_t n )
    in
    {
        assert(n <= this.length);
    }
    body
    {
        if (n)
        {
            if (n == this.length)
            {
                this.chunks = null;
            }
            else
            {
                size_t bytes = 0;

                foreach (i, ref chunk; this.chunks)
                {
                    bytes += chunk.iov_len;
                    if (bytes > n)
                    {
                        size_t d = bytes - n;
                        chunk.iov_base += chunk.iov_len - d;
                        chunk.iov_len  = d;
                        this.chunks = this.chunks[i .. $];
                        break;
                    }
                }
            }
            this.length -= n;
        }

        return this.length;
    }

    /***************************************************************************

        Returns this.chunks[i] as a D array.

    ***************************************************************************/

    void[] opIndex ( size_t i )
    in
    {
        if (i >= this.chunks.length)
        {
            onArrayBoundsError(__FILE__, __LINE__);
        }
    }
    body
    {
        with (this.chunks[i]) return iov_base[0 .. iov_len];
    }

    /***************************************************************************

        Sets this.chunks[i] to reference data.

    ***************************************************************************/

    void[] opIndexAssign ( void[] data, size_t i )
    in
    {
        if (i >= this.chunks.length)
        {
            onArrayBoundsError(__FILE__, __LINE__);
        }
    }
    body
    {
        with (this.chunks[i])
        {
            this.length -= iov_len;
            this.length += data.length;
            iov_len      = data.length;
            iov_base     = data.ptr;
        }

        return data;
    }

    /**************************************************************************/

    import ocean.core.Test: test;

    unittest
    {
        iovec[6] iov_buf;

        void[] a = "Die",
               b = "Katze",
               c = "tritt",
               d = "die",
               e = "Treppe",
               f = "krumm";

        auto iov = typeof(*this)(iov_buf);

        test(iov.chunks.length == iov_buf.length);
        iov[0] = a;
        iov[1] = b;
        iov[2] = c;
        iov[3] = d;
        iov[4] = e;
        iov[5] = f;
        test(iov.length == 27);

        iov.advance(1);
        test(iov.length == 26);
        test(iov.chunks.length == 6);

        test(iov[0] == a[1 .. $]);
        test(iov[1] == b);
        test(iov[2] == c);
        test(iov[3] == d);
        test(iov[4] == e);
        test(iov[5] == f);

        iov.advance(10);
        test(iov.length == 16);
        test(iov.chunks.length == 4);
        test(iov[0] == c[3 .. $]);
        test(iov[1] == d);
        test(iov[2] == e);
        test(iov[3] == f);

        iov.advance(2);
        test(iov.length == 14);
        test(iov.chunks.length == 3);
        test(iov[0] == d);
        test(iov[1] == e);
        test(iov[2] == f);

        iov.advance(14);
        test(!iov.chunks.length);
    }
}
