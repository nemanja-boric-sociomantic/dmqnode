/*******************************************************************************

    Copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

    Classes for reading and writing dht node channel dump files.

*******************************************************************************/

module swarmnodes.mod.dht.storage.memory.DumpFile;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarmnodes.mod.dht.storage.memory.DirectIO;

private import ocean.io.device.DirectIO;

private import ocean.io.FilePath;

private import ocean.io.serialize.SimpleSerializer;

private import tango.io.device.File;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("swarmnodes.mod.dht.storage.memory.DumpFile");
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

public const BackupFileSuffix = ".backup";

public const DeletedFileSuffix = ".deleted";


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

    Replace the existing dump with the new one, while moving the
    existing dump to dump.backup.

    Doing this completely atomically seems to be impossible (link(2)
    fails if the destination file exists), but since we want to avoid
    the situation where we end up with an invalid dump, we prioritize
    always having the plain dump (and updated).

    So, this is the procedure to "rotate" the dump as atomically as
    possible:

    1. Remove dump.backup
    2. Link (hard) dump to dump.backup
    3. Move dump.new to dump

    The worst case ever is losing the dump.backup (if the application
    crashes or the server is rebooted between 1 and 2), but that's being
    backed up already every day.

    The important thing is we never, ever, under any circumstances, end
    up with a regular dump that is either incomplete or inexistent!
    (well, there are always exceptions, like hardware failure or kernel
    bugs ;)

    The downside is now we need disk space to hold 3 times the size of
    the channel instead of 2 times the size of the channel, because at
    some point we have all dump, dump.new and dump.backup all existing
    at the same time.

    Note: dump.new should always exist.

    Params:
        dumped_path = path of the file to which the dump was written (dump.new)
        channel = name of dump file
        root = path of dump files' directory
        path = FilePath object used for file swapping
        swap_path = FilePath object used for file swapping

*******************************************************************************/

public void swapNewAndBackupDumps ( char[] dumped_path, char[] channel,
    FilePath root, FilePath path, FilePath swap_path )
{
    buildFilePath(root, path, channel); // dump
    swap_path.set(path).cat(BackupFileSuffix); // dump.backup

    if ( swap_path.exists )
    {
        // 1. rm dump.backup
        swap_path.remove();
        log.trace("Removed '{}'", swap_path);
    }

    if ( path.exists )
    {
        // 2. ln dump dump.backup
        path.link(swap_path);
        log.trace("Linked '{}' -> '{}'", path, swap_path);
    }

    // 3. mv dump.new dump (new should always exist)
    path.set(dumped_path); // dump.new
    buildFilePath(root, swap_path, channel); // dump
    path.rename(swap_path);
    log.trace("Moved '{}' -> '{}'", dumped_path, swap_path);
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

    ***************************************************************************/

    public this ( ubyte[] buffer )
    {
        this.output = new BufferedDirectWriteTempFile(null, buffer);
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

    Dump file reader.

*******************************************************************************/

public class ChannelLoader
{
    /***************************************************************************

        Input buffered direct I/O file, used to load the channel dumps.

    ***************************************************************************/

    private const BufferedDirectReadFile input;


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
            buffer = buffer used by internal direct I/O reader

    ***************************************************************************/

    public this ( ubyte[] buffer )
    {
        this.input = new BufferedDirectReadFile(null, buffer);
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
        this.input.open(path);

        SimpleSerializer.read(input, this.file_format_version_);
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

    public ulong length ( )
    {
        return (cast(File)this.input.conduit).length - this.file_format_version_.sizeof;
    }


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

