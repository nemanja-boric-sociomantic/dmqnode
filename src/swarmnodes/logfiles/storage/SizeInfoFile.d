/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        27/06/2012: Initial release

    authors:        Gavin Norman

    Maintains a per-channel file storing the number of records and bytes which
    the channel contains.

*******************************************************************************/

module swarmnodes.logfiles.storage.SizeInfoFile;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.io.serialize.SimpleSerializer;

private import tango.io.device.File;

private import tango.io.FilePath;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("swarmnodes.logfiles.storage.SizeInfoFile");
}



/*******************************************************************************

    Sizeinfo file class.  Keeps the total number of records and sum of value
    sizes of all records, and manages a file in the root directory of the
    channel where this information is saved.

*******************************************************************************/

public class SizeInfoFile
{
    /**************************************************************************

        Size info file name

     **************************************************************************/

    public const FileName = "sizeinfo";


    /***************************************************************************

        Struct containing the number of records & bytes, to be de/serialized
        to file.

    ***************************************************************************/

    private align (1) struct SizeInfo
    {
        public ulong num;
        public ulong size;
    }

    private SizeInfo sizeinfo;


    /***************************************************************************

        Channel's base directory.

    ***************************************************************************/

    private char[] base_dir;


    /***************************************************************************

        Path used for checking sizeinfo file's existence.

    ***************************************************************************/

    private const FilePath path;


    /***************************************************************************

        File used to read/write sizeinfo.

    ***************************************************************************/

    private const File file;


    /***************************************************************************

        Flag indicating whether the file is open.

    ***************************************************************************/

    private bool file_open;


    /***************************************************************************

        Constructor.

        Params:
            dir = channel base directory

    ***************************************************************************/

    public this ( char[] dir )
    {
        this.file = new File;
        this.path = new FilePath;

        this.base_dir = dir;
    }


    /**************************************************************************

        Sets the channel's base directory. Called when a storage engine instance
        is recycled and then re-used.

        Params:
            dir = channel base directory

     **************************************************************************/

    public void setDir ( char[] dir )
    {
        this.close();

        this.base_dir = dir;
    }


    /***************************************************************************

        Opens the sizeinfo file. If it already exists, it is deserialized.
        Otherwise a new file is created.

    ***************************************************************************/

    public void open ( )
    {
        assert(!this.file_open, "Sizeinfo file already open");

        this.sizeinfo = SizeInfo.init;

        auto file_exists = this.exists();

        this.file.open(this.path.toString,
            file_exists ? File.ReadWriteOpen : File.ReadWriteCreate);
        this.file_open = true;

        if ( file_exists )
        {
            this.read();

            log.info("Read file in '{}': {} records, {} bytes",
                this.base_dir, this.sizeinfo.num, this.sizeinfo.size);
        }
        else
        {
            this.commit();

            log.info("Created new file in '{}': {} records, {} bytes",
                this.base_dir, this.sizeinfo.num, this.sizeinfo.size);
        }
    }


    /***************************************************************************

        Returns:
            true if the sizeinfo file exists on the disk, false otherwise

    ***************************************************************************/

    public bool exists ( )
    {
        this.path.file = this.FileName;
        this.path.folder = this.base_dir;

        return this.path.exists;
    }


    /***************************************************************************

        Returns:
            the number of records

    ***************************************************************************/

    public ulong num ( )
    {
        return this.sizeinfo.num;
    }


    /***************************************************************************

        Returns:
            the number of bytes

    ***************************************************************************/

    public ulong size ( )
    {
        return this.sizeinfo.size;
    }


    /***************************************************************************

        Current slot and bucket

    ***************************************************************************/

    public void addRecord ( size_t size )
    {
        this.sizeinfo.num++;
        this.sizeinfo.size += size;
    }


    /***************************************************************************

        Writes the size info into the file.

    ***************************************************************************/

    public void commit ( )
    {
        if ( this.file_open )
        {
            this.file.seek(0, File.Anchor.Begin);

            SimpleSerializer.writeData(this.file, &this.sizeinfo,
                this.sizeinfo.sizeof);
        }
    }


    /***************************************************************************

        Closes the file.

    ***************************************************************************/

    public void close ( )
    {
        this.file_open = false;
        this.sizeinfo = SizeInfo.init;
        this.file.close();
    }


    /***************************************************************************

        Reads the size info from the file.

        Throws:
            IOException if the size info file is invalid

    ***************************************************************************/

    private void read ( )
    {
        assert(this.file_open, "Sizeinfo file not open");

        this.file.seek(0, File.Anchor.Begin);

        SimpleSerializer.readData(this.file, &this.sizeinfo,
            this.sizeinfo.sizeof);
    }
}
