/*******************************************************************************

    A wrapper around POSIX file I/O functionality with convenience extensions.

    copyright: Copyright (c) 2016 sociomantic labs. All rights reserved

*******************************************************************************/

module dmqnode.storage.engine.overflow.file.DataFile;

import dmqnode.storage.engine.overflow.file.PosixFile;

class DataFile: PosixFile
{
    import ocean.stdc.posix.unistd: write, pwrite;
    import ocean.stdc.posix.sys.uio: writev;
    import ocean.stdc.posix.sys.types: off_t, ssize_t;

    /***************************************************************************

        Constructor, opens or creates the file using `name` as the file name and
        `dir` as the file directory. `dir` is expected to exist.

        Params:
            name = the file name without directory path
            dir  = the directory for the file, expected to exist

        Throws:
            FileException on error creating or opening the file.

    ***************************************************************************/

    public this ( char[] dir, char[] name )
    {
        super(dir, name);
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
                this.enforce(n > 0, errmsg, file, line);
                left = left[n .. $];
                pos += n;
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
                this.enforce(n > 0, errmsg, file, line);
                left = left[n .. $];
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
            if (ssize_t n = this.restartInterrupted(op(this.fd, data.chunks.ptr, cast(int)data.chunks.length)))
            {
                this.enforce(n > 0, errmsg, file, line);
                data.advance(n);
            }
            else // end of file for read(); write() should
            {    // return 0 iff data.length is 0
                return data.length;
            }
        }

        return 0;
    }
}


/*******************************************************************************

    Vector aka. scatter/gather I/O helper; tracks the byte position if
    readv()/writev() didn't manage to transfer all data with one call.

*******************************************************************************/

struct IoVec
{
    import ocean.stdc.posix.sys.uio: writev, iovec;
    import ocean.core.Exception_tango: onArrayBoundsError;

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
