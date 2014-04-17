/*******************************************************************************

    Copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

    Classes for reading and writing dht node channel dump files.

*******************************************************************************/

module src.mod.dht.storage.memory.DumpFile;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.io.device.DirectIO;

private import ocean.io.serialize.SimpleSerializer;

private import tango.io.device.File;



/*******************************************************************************

    Dump file format version number.

*******************************************************************************/

public const ulong FileFormatVersion = 0;



/*******************************************************************************

    Dump file writer.

*******************************************************************************/

public class ChannelDumper
{
    /***************************************************************************

        Output buffered direct I/O file, used to dump the channels.

    ***************************************************************************/

    private const BufferedDirectWriteFile output;


    /***************************************************************************

        Constructor.

        Params:
            buffer = buffer used by internal direct I/O writer

    ***************************************************************************/

    public this ( ubyte[] buffer )
    {
        this.output = new BufferedDirectWriteFile(null, buffer);
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

