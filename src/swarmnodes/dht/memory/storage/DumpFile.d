/*******************************************************************************

    Copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

    Classes for reading and writing dht node channel dump files.

*******************************************************************************/

module swarmnodes.dht.memory.storage.DumpFile;



/*******************************************************************************

    Imports

*******************************************************************************/


private import swarmnodes.dht.memory.storage.DirectIO;

private import ocean.io.FilePath;

private import ocean.io.serialize.SimpleSerializer;

private import tango.io.model.IConduit : InputStream;

private import tango.io.device.File;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("swarmnodes.dht.memory.storage.DumpFile");
}



/*******************************************************************************

    Dump file format version number.

*******************************************************************************/

public const ulong FileFormatVersion = 0;


/*******************************************************************************

    File suffix constants

*******************************************************************************/

public const DumpFileSuffix = ".tcm";

public const NewFileSuffix = ".dumping";


/*******************************************************************************

    Direct I/O files buffer size.

    See BufferedDirectWriteFile for details on why we use 32MiB.

*******************************************************************************/

public const IOBufferSize = 32 * 1024 * 1024;


/*******************************************************************************

    Formats the file name for a channel into a provided FilePath. The name
    is built using the specified root directory, the name of the channel and
    the standard file type suffix.

    Params:
        root = FilePath object denoting the root dump files directory
        path = FilePath object to set with the new file path
        channel = name of the channel to build the file path for

    Returns:
        The "path" object passed as parameter and properly reset.

*******************************************************************************/

public FilePath buildFilePath ( FilePath root, FilePath path, char[] channel )
{
    path.set(root);
    path.append(channel);
    path.cat(DumpFileSuffix);
    return path;
}


/*******************************************************************************

    Atomically replace the existing dump with the new one.

    Params:
        dumped_path = path of the file to which the dump was written (dump.new)
        channel = name of dump file (without suffix)
        root = path of dump files' directory
        path = FilePath object used for file swapping
        swap_path = FilePath object used for file swapping

*******************************************************************************/

public void rotateDumpFile ( char[] dumped_path, char[] channel, FilePath root,
    FilePath path, FilePath swap_path )
{
    path.set(dumped_path); // dump.new
    buildFilePath(root, swap_path, channel); // dump
    path.rename(swap_path);
}


/*******************************************************************************

    Dump file writer.

*******************************************************************************/

public class ChannelDumper
{
    /***************************************************************************

        Output buffered direct I/O file, used to dump the channels.

    ***************************************************************************/

    private const BufferedDirectWriteTempFile output;


    /***************************************************************************

        Constructor.

        Params:
            buffer = buffer used by internal direct I/O writer
            disable_direct_io = determines if regular buffered I/O (true) or
                                direct I/O is used (false). Regular I/O is only
                                useful for testing, because direct I/O imposes
                                some restrictions over the type of filesystem
                                that can be used.

    ***************************************************************************/

    public this ( ubyte[] buffer, bool disable_direct_io )
    {
        this.output = new BufferedDirectWriteTempFile(null, buffer,
                disable_direct_io);
    }


    /***************************************************************************

        Opens the dump file for writing and writes the file format version
        number at the beginning.

        Params:
            path = path to open

    ***************************************************************************/

    public void open ( char[] path )
    {
        this.output.open(path);

        SimpleSerializer.write(this.output, FileFormatVersion);
    }


    /***************************************************************************

        Returns:
            the path of the open file

    ***************************************************************************/

    public char[] path ( )
    {
        return this.output.path();
    }

    /***************************************************************************

        Writes a record key/value to the file.

        Params:
            key = record key
            value = record value

    ***************************************************************************/

    public void write ( char[] key, char[] value )
    {
        SimpleSerializer.write(this.output, key);
        SimpleSerializer.write(this.output, value);
    }


    /***************************************************************************

        Closes the dump file, writing the requisite end-of-file marker (an empty
        string) at the end.

    ***************************************************************************/

    public void close ( )
    {
        const char[] end_of_file = "";
        SimpleSerializer.write(this.output, end_of_file);

        this.output.close();
    }
}



/*******************************************************************************

    Dump file reader base class.

*******************************************************************************/

abstract public class ChannelLoaderBase
{
    /***************************************************************************

        Input stream, used to load the channel dumps.

    ***************************************************************************/

    protected const InputStream input;


    /***************************************************************************

        Key and value read buffers.

    ***************************************************************************/

    private char[] load_key, load_value;


    /***************************************************************************

        File format version read from beginning of file. Stored so that it can
        be quired by the user (see file_format_version(), below).

    ***************************************************************************/

    private ulong file_format_version_;


    /***************************************************************************

        Constructor.

        Params:
            input = input stream to load channel data from

    ***************************************************************************/

    public this ( InputStream input )
    {
        this.input = input;
    }


    /***************************************************************************

        Opens the dump file for reading and reads the file format version number
        at the beginning.

        NOTE: in the old file format, the first 8 bytes actually store the
        number of records contained in the file.

        Params:
            path = path to open

    ***************************************************************************/

    public void open ( )
    {
        SimpleSerializer.read(this.input, this.file_format_version_);
    }


    /***************************************************************************

        Returns:
            the file format version number read when the file was opened

    ***************************************************************************/

    public ulong file_format_version ( )
    {
        return this.file_format_version_;
    }


    /***************************************************************************

        Returns:
            the number of bytes contained in the file, excluding the 8 byte file
            format version number

    ***************************************************************************/

    final public ulong length ( )
    {
        return this.length_() - this.file_format_version_.sizeof;
    }


    /***************************************************************************

        Returns:
            the number of bytes contained in the file

    ***************************************************************************/

    abstract protected ulong length_ ( );


    /***************************************************************************

        foreach iterator over key/value pairs in the file. Reads until the user
        delegate returns non-0 or a 0-length key is read from the file.

    ***************************************************************************/

    public int opApply ( int delegate ( ref char[] key, ref char[] value ) dg )
    {
        int res;

        // Return true if we have to keep reading
        bool readNextKey ( ref char[] key )
        {
            SimpleSerializer.read(this.input, key);
            return key.length > 0;
        }

        while ( readNextKey(this.load_key) )
        {
            SimpleSerializer.read(this.input, this.load_value);

            res = dg(this.load_key, this.load_value);
            if ( res ) break;
        }

        return res;
    }


    /***************************************************************************

        Closes the dump file.

    ***************************************************************************/

    public void close ( )
    {
        this.input.close();
    }
}



/*******************************************************************************

    Input buffered direct I/O file dump file reader class.

*******************************************************************************/

public class ChannelLoader : ChannelLoaderBase
{
    /***************************************************************************

        Constructor.

        Params:
            buffer = buffer used by internal direct I/O reader
            disable_direct_io = determines if regular buffered I/O (false) or direct
                I/O is used (true). Regular I/O is only useful for testing,
                because direct I/O imposes some restrictions over the type of
                filesystem that can be used.

    ***************************************************************************/

    public this ( ubyte[] buffer, bool disable_direct_io )
    {
        super(new BufferedDirectReadFile(null, buffer, disable_direct_io));
    }

    /***************************************************************************

        Opens the dump file for reading and reads the file format version number
        at the beginning.

        NOTE: in the old file format, the first 8 bytes actually store the
        number of records contained in the file.

        Params:
            path = path to open

    ***************************************************************************/

    public void open ( char[] path )
    {
        (cast(BufferedDirectReadFile)this.input).open(path);

        super.open();
    }


    /***************************************************************************

        Returns:
            the number of bytes contained in the file

    ***************************************************************************/

    override protected ulong length_ ( )
    {
        return (cast(File)this.input.conduit).length;
    }
}

