/*******************************************************************************

    Direct I/O temporary file output

    copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

*******************************************************************************/

module swarmnodes.dht.storage.memory.DirectIO;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.core.Array : concat;

private import ocean.io.device.DirectIO;

private import tango.stdc.posix.fcntl : O_DIRECT; // Linux only



/*******************************************************************************

    System calls. (Not defined in tango.)

*******************************************************************************/

extern ( C )
{
    int mkostemp(char *path_template, int flags);
}



/*******************************************************************************

    Buffered file to do direct I/O writes to a temporary file. The temporary
    file is created using the linux mkostemp() function, which appends a unique
    6-character string to the end of the specified path.

    Please read the module documentation for details.

*******************************************************************************/

public class BufferedDirectWriteTempFile : BufferedDirectWriteFile
{
    /***************************************************************************

        Temp file to do direct IO writes.

    ***************************************************************************/

    static protected class DirectWriteTempFile : DirectWriteFile
    {
        /***********************************************************************

            String which contains name of temporary file (filled in by the call
            to mkostemp(), in open()).

            Ideally this would be stored directly in super.path_ (see File), but
            that field is private and has no setter, beyond calling open(),
            which we're explicitly avoiding here.

        ***********************************************************************/

        private char[] temp_file_path;

        /***********************************************************************

            Opens a temporary file at the specified path. The file will have a
            unique 6-character string appended to the path.

            Params:
                path = path at which to create temporary file

            Throws:
                IOException on error opening the file

        ***********************************************************************/

        public override void open ( char[] path )
        {
            this.temp_file_path.concat(path, "XXXXXX\0");
            auto fd = mkostemp(this.temp_file_path.ptr, O_DIRECT);
            if ( fd == -1 )
            {
                this.error(); // throws an IOException
            }

            // the oddly-named 'reopen' allows us to set the Device's fd
            this.reopen(cast(Handle)fd);
        }

        /***********************************************************************

            Returns:
                the file's path

        ***********************************************************************/

        public override char[] path ( )
        {
            return this.temp_file_path;
        }
    }

    /***************************************************************************

        Constructs a new BufferedDirectWriteTempFile.

        If a path is specified, a temporary file is opened there.

        See documentation for super(char[], ubyte[]) for details.

        Params:
            path = Path of the file to write to.
            buffer = Buffer to use for writing, the length must be multiple of
                     the BLOCK_SIZE and the memory must be aligned to the
                     BLOCK_SIZE

    ***************************************************************************/

    public this ( char[] path, ubyte[] buffer )
    {
        super(path, buffer);
    }

    /***************************************************************************

        Constructs a new BufferedDirectWriteTempFile allocating a new buffer.

        If a path is specified, a temporary file is opened there.

        See documentation for super(char[], ubyte[]) for details.

        Params:
            path = Path of the file to write to.
            buffer_blocks = Buffer size in blocks (default 32MiB)

    ***************************************************************************/

    public this ( char[] path = null, size_t buffer_blocks = 32 * 2 * 1024 )
    {
        super(path, new ubyte[buffer_blocks * BLOCK_SIZE]);
    }

    /***************************************************************************

        Instantiates the file object to be used to write to. Overrides the base
        class' implementation to return a temp file instead.

        Returns:
            file object to write to

    ***************************************************************************/

    protected override DirectWriteFile newFile ( )
    {
        return new DirectWriteTempFile;
    }
}


