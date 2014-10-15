/******************************************************************************

    Output implementation for Distributed Hashtable LogFiles storage engine

    copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved

    version:        August 2010: Initial release

    authors:        David Eckardt

    Implements the PutDup database command and keeps track of the number of
    records and the total size of all record values.
    On each write commit the record number and size information is written to a
    file named according to LogRecord.SizeInfoFileName in the base directory. On
    instantiation this information is read from that file, if the file exists,
    otherwise the number and size values are initialized to 0.

    See swarmnodes.logfiles.storage.LogRecord for a description of the database
    file organization and slot/bucket association.

 ******************************************************************************/

module swarmnodes.logfiles.storage.LogRecordPut;

/******************************************************************************

    Imports

 ******************************************************************************/

private import swarmnodes.logfiles.storage.LogRecord;

private import swarmnodes.logfiles.storage.SizeInfoFile;

private import swarm.dht.DhtHash;

private import ocean.core.Array : copy;
private import ocean.core.Exception: assertEx;

private import tango.io.model.IConduit: OutputStream;

private import tango.io.stream.Buffered;

private import tango.io.device.File;

private import tango.io.FilePath;

private import tango.core.Exception: IOException;



/******************************************************************************/

public class LogRecordPut
{
    /**************************************************************************

        Default output buffer size: 64 kB

     **************************************************************************/

    const size_t DefaultBufferSize = 64 * 1024;

    /**************************************************************************

        This alias for chainable methods

     **************************************************************************/

    alias typeof(this) This;

    /**************************************************************************

        SlotBucket struct

        Calculates and keeps slot and bucket from a key

     **************************************************************************/

    private struct SlotBucket
    {
        public hash_t slot;
        public hash_t bucket;

        public typeof (this) fromKey ( hash_t key )
        {
            this.bucket = key >> LogRecord.SplitBits.key_bits;
            this.slot = bucket >> LogRecord.SplitBits.bucket_bits;

            return this;
        }
    }

    /**************************************************************************

        Current slot and bucket, used to check when a record is being written to
        a different file.

     **************************************************************************/

    private SlotBucket current_sb;

    /**************************************************************************

        Sizeinfo file.

     **************************************************************************/

    private SizeInfoFile sizeinfo_file;

    /**************************************************************************

        Bucket file open indicator: true indicates that a bucket file is
        currently open

     **************************************************************************/

    private bool file_open;

    /**************************************************************************

        Base directory name

     **************************************************************************/

    private char[] base_dir;

    /**************************************************************************

        File and FilePath instance, resued in many functions

     **************************************************************************/

    private File file;
    private FilePath path;

    /**************************************************************************

        Output buffer

     **************************************************************************/

    private BufferedOutput output;

    /**************************************************************************

        Constructor; uses the default output buffer size

        Params:
            dir = working directory

        Throws:
            IOException if the size info file is invalid

     **************************************************************************/

    public this ( char[] dir )
    {
        this(dir, this.DefaultBufferSize);
    }

    /**************************************************************************

        Constructor

        Params:
            dir = channel base directory
            buffer_size = output buffer size

        Throws:
            IOException if the size info file is invalid

     **************************************************************************/

    public this ( char[] dir, size_t buffer_size )
    {
        this.file = new File;
        this.output = new BufferedOutput(this.file, buffer_size);

        this.path = new FilePath;

        this.base_dir.copy(dir);
        this.sizeinfo_file = new SizeInfoFile(this.base_dir);
    }

    /**************************************************************************

        Sets the channel's base directory (called when a storage engine instance
        is recycled and then re-used). The directory is passed on to the size
        info file class.

        Params:
            dir = channel base directory

     **************************************************************************/

    public void setDir ( char[] dir )
    {
        this.base_dir.copy(dir);
        this.sizeinfo_file.setDir(this.base_dir);

        this.sizeinfo_file.open();
    }

    /**************************************************************************

        Disposer

     **************************************************************************/

    override public void dispose ( )
    {
        delete this.file;
        delete this.path;
        delete this.sizeinfo_file;
        delete this.output;
    }

    /**************************************************************************

        Appends a record to the bucket file that corresponds to key.

        Params:
            key = record key
            value = record value

        Returns:
            this instance

     **************************************************************************/

    public This putDup ( hash_t key, char[] value )
    {
        this.openFile(key);

        this.writeRecord(key, value);

        return this;
    }

    /**************************************************************************

        Flushes the write buffer, closes the current bucket file and writes the
        size info.

        Returns:
            this instance

     **************************************************************************/

    public This commit ( )
    {
        this.sizeinfo_file.commit();

        if ( this.file_open )
        {
            this.closeFile();
        }

        return this;
    }

    /***************************************************************************

        Commits any pending writes to disk, then removes all files and the
        working directory.

        Returns:
            this instance

    ***************************************************************************/

    public This clear ( )
    {
        this.commit();

        LogRecord.removeFiles(this.base_dir);

        this.sizeinfo_file.close();

        this.removeBaseDir();

        return this;
    }

    /**************************************************************************

        Returns:
            the number of records currently in database

     **************************************************************************/

    public ulong numRecords ( )
    {
        return this.sizeinfo_file.num;
    }

    /**************************************************************************

        Returns:
            the sum of the sizes of all records currently in database

     **************************************************************************/

    public ulong size ( )
    {
        return this.sizeinfo_file.size;
    }

    /**************************************************************************

        Appends a record to the bucket file that corresponds to key.

        Params:
            key = record key
            value = record value

        Returns:
            this instance

     **************************************************************************/

    private void writeRecord ( hash_t key, char[] value )
    {
        LogRecord.RecordHeader header;

        header.key = key;
        header.len = value.length;

        this.output.append(&header, header.sizeof);
        this.output.append(value.ptr, value.length * typeof (value[0]).sizeof);

        this.sizeinfo_file.addRecord(value.length);
    }

    /**************************************************************************

        Opens the bucket file that corresponds to key or creates a new one if
        not existing. Sets the current slot/bucket corresponding to key.
        If a bucket file is currently open and the current slot/bucket
        correspond to key, nothing is done.

        Params:
            key = record key

     **************************************************************************/

    private void openFile ( hash_t key )
    {
        SlotBucket sb;

        sb.fromKey(key);

        if (this.current_sb != sb || !this.file_open)
        {
            this.commit();

            char[LogRecord.SplitBits.slot_digits] slot_hex;
            char[LogRecord.SplitBits.bucket_digits] bucket_hex;

            // Create slot directory if it doesn't exist
            this.path.file = "";
            this.path.folder = DhtHash.intToHex(sb.slot, slot_hex);
            this.path.prepend = this.base_dir;

            if (!this.path.exists)
            {
                this.path.create();
            }

            // Create sizeinfo file if it doesn't exist
            if ( !this.sizeinfo_file.exists() )
            {
                this.sizeinfo_file.open();
            }

            // Open bucket file.
            this.path.file = DhtHash.intToHex(sb.bucket, bucket_hex);
            this.file.open(this.path.toString, File.WriteAppending);

            this.current_sb = sb;
        }

        this.file_open = true;
    }

    /**************************************************************************

        Closes the current bucket file after flushing the output buffer.

     **************************************************************************/

    private void closeFile ( )
    in
    {
        assert (this.file_open, typeof (this).stringof ~ ".closeFile: file not open");
    }
    body
    {
        this.output.flush();
        this.file.close();

        this.file_open = false;
    }

    /***************************************************************************

        Removes the base data directory. This method will do nothing if the
        directory is not empty.

    ***************************************************************************/

    private void removeBaseDir ( )
    {
        this.path.folder = this.base_dir;
        if ( this.path.exists() )
        {
            this.path.remove();
        }
    }
}

