/*******************************************************************************

    Dirct I/O reader and writter

    copyright:      Copyright (c) 2013 sociomantic labs. All rights reserved

    authors:        Leandro Lucarella

    This module should be TEMPORARY, until we get a proper, general purpose,
    async I/O library. This is why is hidden in the depths of swarm and is not
    in ocean.

*******************************************************************************/

module src.mod.dht.storage.memory.DirectIO;



/*******************************************************************************

    Imports

*******************************************************************************/

private import tango.io.model.IConduit;

private import tango.io.device.File;

private import tango.core.Exception: IOException;

private import tango.stdc.posix.fcntl : O_DIRECT; // Linux only



/*******************************************************************************

    Mixin template for classes that need to have buffers that are aligned to
    a certain block size and don't support some operations.

*******************************************************************************/

private template AlignedBufferedStream ( )
{

    /***********************************************************************

        Block size.

        Almost every HDD out there has a block size of 512. But we should be
        careful about this...

    ***********************************************************************/

    public enum { BLOCK_SIZE = 512 }

    /***********************************************************************

        Internal buffer (the size needs to be multiple of the block size).

    ***********************************************************************/

    protected ubyte[] buffer;

    /***********************************************************************

        Internal pointer to the next byte of the buffer that is free.

    ***********************************************************************/

    protected size_t free_index;

    /***********************************************************************

        Construct the buffer using an existing buffer.

        This method checks the buffer is properly aligned and the lenght is
        a multiple of BLOCK_SIZE too.

        Params:
            buffer = buffer to re-use for this aligned buffer.

    ***********************************************************************/

    protected void setBuffer ( ubyte[] buffer )
    {
        this.buffer = buffer;
        // Throw an error if the buffer is not aligned to BLOCK_SIZE
        if ( !this.isAligned(buffer.ptr) )
            throw new IOException("Buffer is not aligned to BLOCK_SIZE, maybe "
                    "you should start using posix_memalign(3)");
        // Throw an error if the buffer length is not a multiple of the
        // BLOCK_SIZE
        if ((buffer.length % BLOCK_SIZE) != 0)
            throw new IOException("Buffer length is not multiple of the "
                    "BLOCK_SIZE");
        this.free_index = 0;
    }

    /***********************************************************************

        Construct the buffer with a specified size.

        This method checks the buffer is properly aligned and the lenght is
        a multiple of BLOCK_SIZE too.

        Params:
            buffer_blocks = Buffer size in blocks

    ***********************************************************************/

    protected void createBuffer ( size_t buffer_blocks )
    {
        // O_DIRECT needs to work with aligned memory (to the block size,
        // which 99.9% of the time is 512), but the current GC implementation
        // always align memory for a particular block size (and 512 is a current
        // GC block size), so if the buffer is 512 or bigger, we are just fine.
        //
        // If we can't rely on this eventually, we can use posix_memalign(3)
        // instead to allocate the memory.
        this.setBuffer(new ubyte[buffer_blocks * BLOCK_SIZE]);
    }

    /***********************************************************************

        Return true if the pointer is aligned to the block size.

    ***********************************************************************/

    final public bool isAligned ( void* ptr )
    {
        return (cast(size_t) ptr & (this.BLOCK_SIZE - 1)) == 0;
    }

    /***********************************************************************

        Throws an IOException because is not implemented.

    ***********************************************************************/

    public override long seek (long offset, Anchor anchor = Anchor.Begin)
    {
        throw new IOException("seek() not supported by " ~
                this.classinfo.name);
    }

    /***********************************************************************

        Throws an IOException because is not implemented.

    ***********************************************************************/

    public override IOStream flush ()
    {
        throw new IOException("flush() not supported by " ~
                this.classinfo.name);
    }

    /***********************************************************************

        Throws IOException because is not implemented.

        Only present in OutputStream, so we can't use the override keyword.

    ***********************************************************************/

    public OutputStream copy (InputStream src, size_t max = -1)
    {
        throw new IOException("copy() not supported by " ~
                this.classinfo.name);
    }

}


/*******************************************************************************

    Buffered file to do direct IO writes.

    We need to use direct IO to avoid the page cache to freeze any other IO when
    dumping (see #3). O_SYNC is not really needed, is just to have an extra
    guarantee that when the write is done, the stuff will be really on the disk.

    #3 https://github.com/sociomantic/swarm/issues/3

    NOTE: This should be only a temporary hack until a more general solution is
          found for both direct and async I/O.
          Also bare in mind that this buffered file will add padding bytes at
          the end of the file to make the size a multiple of the BLOCK_SIZE. The
          file format of the stored data should be resilient to this. We could
          do a trick and truncate() the file when is closed to avoid the extra
          bytes, but we need to keep track of the "real size" if we do this, and
          since this should be temporary it doesn't seems to worth the trouble.

*******************************************************************************/

public class BufferedDirectWriteFile: OutputStream
{

    /*******************************************************************************

        File to do direct IO writes.

        Actually there is no way to open files with tango specifying custom
        flags that is not sub-classing. Bummer!

    *******************************************************************************/

    static private class DirectWriteFile : File
    {
        void open(char[] path)
        {
            if (!super.open(path, this.WriteCreate, O_DIRECT))
                this.error();
        }
    }

    /***********************************************************************

        Direct I/O file device to write to.

    ***********************************************************************/

    private DirectWriteFile file;

    /***********************************************************************

        Constructs a new BufferedDirectWriteFile.

        If a path is specified, the file is open too. A good buffer size depends
        mostly on the speed of the disk (memory and CPU). If the buffer is too
        big, you will notice that writing seems to happen in long bursts, with
        periods of a lot of buffer copying, and long wait periods writing to
        disk. If the buffer is too small, the throughput will be too small,
        resulting in bigger total write time.

        32MiB have shown to be a decent value for a low end magnetic hard drive.

        Params:
            path = Path of the file to write to.
            buffer = Buffer to use for writing, the length must be multiple of
                     the BLOCK_SIZE and the memory must be aligned to the
                     BLOCK_SIZE

    ***********************************************************************/

    public this (char[] path, ubyte[] buffer)
    {
        this.setBuffer(buffer);
        this.file = new DirectWriteFile;
        if (path.length > 0)
            this.file.open(path);
    }

    /***********************************************************************

        Constructs a new BufferedDirectWriteFile.

        If a path is specified, the file is open too. A good buffer size depends
        mostly on the speed of the disk (memory and CPU). If the buffer is too
        big, you will notice that writing seems to happen in long bursts, with
        periods of a lot of buffer copying, and long wait periods writing to
        disk. If the buffer is too small, the throughput will be too small,
        resulting in bigger total write time.

        32MiB have shown to be a decent value for a low end magnetic hard drive.

        Params:
            path = Path of the file to write to.
            buffer_blocks = Buffer size in blocks (default 32MiB)

    ***********************************************************************/

    public this (char[] path = null, size_t buffer_blocks = 32 * 2 * 1024)
    {
        // O_DIRECT needs to work with aligned memory (to the block size,
        // which 99.9% of the time is 512), but the current GC implementation
        // always align memory for a particular block size (and 512 is a current
        // GC block size), so if the buffer is 512 or bigger, we are just fine.
        //
        // If we can't rely on this eventually, we can use posix_memalign(3)
        // instead to allocate the memory.
        this(path, new ubyte[buffer_blocks * BLOCK_SIZE]);
    }

    /***********************************************************************

        Mixin for common functionality.

    ***********************************************************************/

    mixin AlignedBufferedStream;

    /***********************************************************************

        Open a BufferedDirectWriteFile file.

        Params:
            path = Path of the file to write to.

    ***********************************************************************/

    public void open (char[] path)
    {
        assert (this.file.fileHandle == -1);
        this.file.open(path);
        this.free_index = 0;
    }

    /***********************************************************************

        Return the host conduit.

    ***********************************************************************/

    public IConduit conduit ()
    {
        return this.file;
    }

    /***********************************************************************

        Close the underlying file, but calling flushWithPadding() and sync()
        first.

    ***********************************************************************/

    public void close ()
    {
        if (this.file.fileHandle == -1)
            return;
        this.flushWithPadding();
        this.sync();
        this.file.close();
    }

    /***********************************************************************

        Write to stream from a source array. The provided src content will be
        written to the stream.

        Returns the number of bytes written from src, which may be less than the
        quantity provided. Eof is returned when an end-of-flow condition arises.

    ***********************************************************************/

    public size_t write (void[] src)
    {
        assert (this.file.fileHandle != -1);

        size_t total = src.length;

        if (src.length == 0)
            return 0;

        // Optimization: avoid extra copy if src is already aligned to the
        // block size
        if (this.free_index == 0)
        {
            while (src.length >= this.buffer.length)
            {
                if (this.isAligned(src.ptr))
                {
                    this.file.write(src[0 .. this.buffer.length]);
                    src = src[this.buffer.length .. $];
                }
            }
        }

        while (this.free_index + src.length > this.buffer.length)
        {
            auto hole = this.buffer.length - this.free_index;
            this.buffer[this.free_index .. $] = cast(ubyte[]) src[0 .. hole];
            this.free_index = this.buffer.length;
            this.flushWithPadding();
            src = src[hole .. $];
        }

        this.buffer[this.free_index .. this.free_index + src.length] =
                cast(ubyte[]) src[0 .. $];
        this.free_index = this.free_index + src.length;

        return total;
    }

    /***********************************************************************

        Return the upstream sink.

    ***********************************************************************/

    public OutputStream output ()
    {
        return file;
    }

    /**********************************************************************

        Write the current buffer rounding to the block size (and setting the
        padding bytes to padding_byte).

        Params:
            padding_byte = Byte to use to fill the padding.

        Returns:
            Number of bytes that have been flushed.

    **********************************************************************/

    public size_t flushWithPadding ( ubyte padding_byte = 0 )
    {
        assert (this.file.fileHandle != -1);

        if (this.free_index == 0)
            return 0;

        if ((this.free_index % this.BLOCK_SIZE) != 0)
        {
            auto hole = BLOCK_SIZE - this.free_index % BLOCK_SIZE;
            this.buffer[this.free_index .. this.free_index+hole] = padding_byte;
            this.free_index += hole;
        }

        size_t written = 0;
        while (written < this.free_index)
        {
            written =+ this.file.write(buffer[written .. this.free_index]);
        }

        this.free_index = 0;

        return written;
    }

    /**********************************************************************

        Instructs the OS to flush it's internal buffers to the disk device.

    **********************************************************************/

    public void sync ( )
    {
        assert (this.file.fileHandle != -1);
        this.file.sync();
    }

}


/*******************************************************************************

    Buffered file to do direct IO reads.

    Tango's BufferedInput works with an internal buffer, which you really can't
    change. To be able to reuse the I/O buffer we need to create a new class,
    and while we are at it, is better to use O_DIRECT for this too to avoid
    unnecessary page cache activity.

    Please see BufferedDirectWriteFile comments too for more details on
    O_DIRECT.

*******************************************************************************/

public class BufferedDirectReadFile: InputStream
{

    /*******************************************************************************

        File to do direct IO reads.

        Actually there is no way to open files with tango specifying custom
        flags that is not sub-classing. Bummer!

    *******************************************************************************/

    static private class DirectReadFile : File
    {
        void open(char[] path)
        {
            if (!super.open(path, this.ReadExisting, O_DIRECT))
                this.error();
        }
    }

    /***********************************************************************

        Direct I/O file device to read from.

    ***********************************************************************/

    private DirectReadFile file;

    /***********************************************************************

        Internal pointer to data we already read but is still pending, waiting
        for a reader.

    ***********************************************************************/

    protected size_t pending_index;

    /***********************************************************************

        Constructs a new BufferedDirectReadFile.

        See notes in BufferedDirectWriteFile about the default buffer size.

        Params:
            path = Path of the file to read from.
            buffer = Buffer to use for reading, the length must be multiple of
                     the BLOCK_SIZE and the memory must be aligned to the
                     BLOCK_SIZE

    ***********************************************************************/

    public this (char[] path, ubyte[] buffer)
    {
        this.setBuffer(buffer);
        this.pending_index = 0;
        this.file = new DirectReadFile;
        if (path.length > 0)
            this.file.open(path);
    }

    /***********************************************************************

        Constructs a new BufferedDirectReadFile.

        See notes in BufferedDirectWriteFile about the default buffer size.

        Params:
            path = Path of the file to read from.
            buffer_blocks = Buffer size in blocks (default 32MiB)

    ***********************************************************************/

    public this (char[] path = null, size_t buffer_blocks = 32 * 2 * 1024)
    {
        // See comments on BufferedDirectWriteFile constructor
        this(path, new ubyte[buffer_blocks * BLOCK_SIZE]);
    }

    /***********************************************************************

        Mixin for common functionality.

    ***********************************************************************/

    mixin AlignedBufferedStream;

    /***********************************************************************

        Open a BufferedDirectReadFile file.

        Params:
            path = Path of the file to read from.

    ***********************************************************************/

    public void open (char[] path)
    {
        assert (this.file.fileHandle == -1);
        this.file.open(path);
        this.free_index = 0;
        this.pending_index = 0;
    }

    /***********************************************************************

        Return the host conduit.

    ***********************************************************************/

    public IConduit conduit ()
    {
        return this.file;
    }

    /***********************************************************************

        Close the underlying file, but calling sync() first.

    ***********************************************************************/

    public void close ()
    {
        if (this.file.fileHandle == -1)
            return;
        this.sync();
        this.file.close();
    }

    /***********************************************************************

        Read from stream to a destination array. The content read from the
        stream will be stored in the provided dst.

        Returns the number of bytes written to dst, which may be less than
        dst.length. Eof is returned when an end-of-flow condition arises.

    ***********************************************************************/

    public size_t read (void[] dst)
    {
        assert (this.file.fileHandle != -1);

        if (dst.length == 0)
            return 0;

        size_t bytes_read = 0;

        // Read from pending data (that was read in a previous read())
        auto pending_len = this.free_index - this.pending_index;
        if (pending_len > 0)
        {
            if (dst.length <= pending_len)
            {
                pending_len = dst.length;
            }

            bytes_read += pending_len;
            dst[0 .. pending_len] = this.buffer[this.pending_index ..
                                                this.pending_index + pending_len];
            this.pending_index += pending_len;
            dst = dst[pending_len .. $];
        }

        // Reset if we don't have pending data to make next read more efficient
        if (this.pending_index == this.free_index)
        {
            this.free_index = 0;
            this.pending_index = 0;
        }

        // There is no pending data at this point, we work only with the
        // free_index. Also, we know free_index and pending_index got reset to 0

        // Optimization: avoid extra copy if dst is already aligned to the
        // block size
        if (this.free_index == 0 && this.isAligned(dst.ptr))
        {
            while (dst.length >= this.buffer.length)
            {
                auto r = this.file.read(dst[0 .. this.buffer.length]);

                if (r == this.file.Eof)
                {
                    return bytes_read ? bytes_read : r;
                }

                bytes_read += r;
                dst = dst[r .. $];
            }
        }

        // Read whole buffer chunks as long as needed
        while (dst.length > 0)
        {
            auto r = this.file.read(buffer);

            if (r == this.file.Eof)
            {
                return bytes_read ? bytes_read : r;
            }

            // Pass to the upper-level as if we just had read dst.length if we
            // read more (and set the internal pending data state properly)
            if (r >= dst.length)
            {
                this.pending_index = dst.length;
                this.free_index = r;
                r = dst.length;
            }

            bytes_read += r;
            dst[0 .. r] = buffer[0 .. r];
            dst = dst[r .. $];
        }

        return bytes_read;
    }

    /***********************************************************************

        Throws IOException because is not implemented.

    ***********************************************************************/

    void[] load (size_t max = -1)
    {
        throw new IOException("load() not supported by " ~
                this.classinfo.name);
    }

    /***********************************************************************

        Return the upstream sink.

    ***********************************************************************/

    public InputStream input ()
    {
        return file;
    }

    /**********************************************************************

        Instructs the OS to flush it's internal buffers to the disk device.

    **********************************************************************/

    public void sync ( )
    {
        assert (this.file.fileHandle != -1);
        this.file.sync();
    }

}

