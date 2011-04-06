/*******************************************************************************

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        March 2011: Initial release

    authors:        Gavin Norman

    Log files storage engine scanner / repairer.

    All repaired files are backed up with '.backup' appended to the filename,
    before any modifications are made.

*******************************************************************************/

module src.mod.repair.storagerepair.LogFilesRepair;



/*******************************************************************************

    Imports

*******************************************************************************/

private import src.mod.repair.storagerepair.model.IStorageRepair;

private import Array = ocean.core.Array;

private import ocean.core.Exception;

private import ocean.io.serialize.SimpleSerializer;

private import tango.io.Stdout;
private import tango.io.FilePath;
private import tango.io.Path;

private import tango.io.device.File;

private import Integer = tango.text.convert.Integer;



/*******************************************************************************

    Log files repairer.

*******************************************************************************/

public class LogFilesRepair : IStorageRepair
{
    /***************************************************************************

        String to append to name of backed-up files.
    
    ***************************************************************************/

    private const backup_id = ".backup";


    /***************************************************************************

        Struct describing a file in which damaged data has been foound.
    
    ***************************************************************************/

    private struct DamagedChunkInfo
    {
        /***********************************************************************

            Name of damaged file

        ***********************************************************************/

        public char[] filename;

        
        /***********************************************************************

            Position through file where damage detected.
    
        ***********************************************************************/

        public size_t broken_pos;

        
        /***********************************************************************

            Estimate of the number of records occurring in the file after the
            damaged area.
    
        ***********************************************************************/

        public ulong lost_records;
    }


    /***************************************************************************

        List of damaged chunk files found during scanning.
    
    ***************************************************************************/

    private DamagedChunkInfo[] damaged_chunks;


    /***************************************************************************

        Channel to scan / repair.
    
    ***************************************************************************/

    private char[] channel;

    
    /***************************************************************************

        Hash range to scan / repair.
    
    ***************************************************************************/

    private hash_t start, end;


    /***************************************************************************

        Executes a scan / repair over the files for the specified hash range in
        the specified channel.
    
        Params:
            channel = channel to scan / repair
            start = start of hash range to scan / repair
            end = end of hash range to scan / repair
            mode = process mode (see enum)
    
    ***************************************************************************/

    public void process ( char[] channel, hash_t start, hash_t end, Mode mode )
    {
        this.channel = channel;
        this.start = start;
        this.end = end;

        switch ( mode )
        {
            case Mode.Check:
                this.scanForDamage();
            break;

            case Mode.Repair:
                this.repairDamage();
            break;

            default:
        }
    }


    /***************************************************************************

        Scans log files for damage.
    
    ***************************************************************************/

    private void scanForDamage ( )
    {
        Stdout.formatln("\nScanning channel data/{} in range 0x{:x8}..0x{:x8}", this.channel, this.start, this.end);

        this.damaged_chunks.length = 0;
        this.processChannel(this.channel, &this.scanBucket);

        Stdout.formatln("\nFound {} damaged chunks files...", this.damaged_chunks.length);
        foreach ( damaged_chunk; this.damaged_chunks )
        {
            Stdout.formatln("  {} (position {}) - approx. {} inaccessible records",
                    damaged_chunk.filename, damaged_chunk.broken_pos, damaged_chunk.lost_records);
        }
    }


    /***************************************************************************

        Scans a bucket directory for damaged chunk files.

        Params:
            bucket = bucket directory info
            progress = fraction denoting which bucket this is out of all
                existing buckets

    ***************************************************************************/

    private void scanBucket ( FS.FileInfo bucket, float progress )
    {
        Stdout.formatln("Scanning  bucket {}{}   ({}%)", bucket.path, bucket.name, 100 * progress);
        this.processBucket(bucket, &this.scanChunk);
    }


    /***************************************************************************

        Scans a chunk file for damaged records.

        Params:
            chunk = chunk file info
            progress = fraction denoting which chunk this is out of all
                existing chunks

    ***************************************************************************/

    private void scanChunk ( FS.FileInfo chunk, float progress )
    {
        Stdout.formatln("Scanning   chunk {}{}   ({}%)", chunk.path, chunk.name, 100 * progress);

        scope file = new File(chunk.path ~ chunk.name, File.ReadExisting);
        scope ( exit ) file.close;

        auto file_length = file.length;
        auto file_pos = file.position;

        ulong record_count;

        while ( file_pos < file_length )
        {
            hash_t key;
            size_t len;
            SimpleSerializer.read(file, key);
            SimpleSerializer.read(file, len);

            auto record_size = key.sizeof + len.sizeof + len;

            if ( file_pos + record_size > file_length )
            {
                auto percent_thru_file = cast(double)file_pos / cast(double)file_length;
                auto total_records_guess = cast(double)record_count / percent_thru_file;

                this.damaged_chunks.length = this.damaged_chunks.length + 1;
                Array.copy(this.damaged_chunks[$-1].filename, file.toString);
                this.damaged_chunks[$-1].broken_pos = file_pos;
                this.damaged_chunks[$-1].lost_records = cast(ulong)((1 - percent_thru_file) * total_records_guess);

                Stdout.formatln("           Found bad record! Claims to be {} bytes, but the file only has {} bytes remaining, key = 0x{:x}, file pos = {}%",
                        len, file_length - (file_pos + key.sizeof + len.sizeof), key, 100 * percent_thru_file);

                return; // can't process this file any further
            }

            file.seek(len, File.Anchor.Current);
            file_pos += record_size;
            record_count++;
        }
    }


    /***************************************************************************

        Scans log files for damage, repairs any damaged chunks, then re-scans to
        confirm that the repair worked.

    ***************************************************************************/

    private void repairDamage ( )
    {
        this.scanForDamage();

        if ( this.damaged_chunks.length )
        {
            Stdout.formatln("\nRepairing damaged chunk files...");

            foreach ( damaged_chunk; this.damaged_chunks )
            {
                this.repairChunk(damaged_chunk.filename, damaged_chunk.broken_pos);
            }

            this.scanForDamage();

//            this.checkRepair();
//            this.updateSizeInfo(channel, this.records, this.bytes);
        }
        else
        {
            Stderr.formatln("\nNothing to repair");
        }
    }


    /***************************************************************************

        Repairs a damaged chunk file. A backup of the file is made with
        '.backup' appended to the filename, before any modifications are made.

        Params:
            filename = name of damaged file
            broken_pos = position of damage in file

    ***************************************************************************/

    private void repairChunk ( char[] filename, ulong broken_pos )
    {
        // Backup the file
        this.backupFile(filename);

        // Truncate and save
        scope file = new File(filename, File.ReadWriteExisting);
        scope ( exit ) file.close;

        auto original_length = file.length;
        file.truncate(broken_pos);
        auto fixed_length = file.length;

        Stdout.formatln("Repaired chunk {}, cut off the last {} bytes", filename, original_length - fixed_length);
    }


    /***************************************************************************

        Processes each bucket directory which exists in a channel, calling the
        provided delegate for each of them.

        Params:
            channel = name of channel to process
            bucket_dg = delegate to be called over each bucket directory found
                in the channel

    ***************************************************************************/

    private void processChannel ( char[] channel, void delegate ( FS.FileInfo, float ) bucket_dg )
    {
        scope path = new FilePath("data/" ~ channel);
        assertEx(path.exists, "Specified channel doesn't exist");
        assertEx(path.isFolder, "Specified channel isn't a folder");

        uint num_children;
        foreach ( child; path )
        {
            num_children++;
        }

        uint i = 1;
        foreach ( child; path )
        {
            if ( child.folder && this.bucketInRange(child.name, this.start, this.end) )
            {
                bucket_dg(child, cast(float)i / cast(float)num_children);
            }
            i++;
        }
    }


    /***************************************************************************

        Processes each chunk file which exists in a bucket directory, calling
        the provided delegate for each of them.

        Params:
            bucket = bucket directory info
            chunk_dg = delegate to be called over each chunk file found in the
                bucket directory

    ***************************************************************************/

    private void processBucket ( FS.FileInfo bucket, void delegate ( FS.FileInfo, float ) chunk_dg )
    {
        scope path = new FilePath(bucket.path ~ bucket.name);
        assertEx(path.exists, "Specified bucket '" ~ path.toString ~ "' doesn't exist");
        assertEx(path.isFolder, "Bucket " ~ path.toString ~ "isn't a folder");

        uint num_children;
        foreach ( child; path )
        {
            num_children++;
        }

        uint i = 1;
        foreach ( child; path )
        {
            if ( !child.folder && !this.isBackup(child.name) && this.chunkInRange(child.name, this.start, this.end) )
            {
                chunk_dg(child, cast(float)i / cast(float)num_children);
            }
            i++;
        }
    }


    /***************************************************************************

        Creates a backup of the specified file, adding ".backup" to its name.

        Params:
            filename = file to backup

    ***************************************************************************/

    private void backupFile ( char[] filename )
    {
        scope original = new FilePath(filename);
        assertEx(original.exists, "attempting to backup a file which doesn't exist! - " ~ filename);

        scope backup = new FilePath(filename ~ this.backup_id);
        backup.createFile();
        backup.copy(filename);
    }


    /***************************************************************************

        Tells whether the given file is a backup file created by a previous run
        of this program. (This is used to avoid scanning chunk backups.)

        Params:
            filename = file to check

    ***************************************************************************/

    private bool isBackup ( char[] filename )
    {
        if ( filename.length < this.backup_id.length )
        {
            return false;
        }
        else
        {
            return filename[$ - this.backup_id.length .. $] == this.backup_id;
        }
    }


    /***************************************************************************

        Creates a backup of the specified file, adding ".backup" to its name.

        Params:
            filename = file to backup

    ***************************************************************************/

    private bool bucketInRange ( char[] bucket_name, hash_t start, hash_t end )
    in
    {
        assert(start <= end);
    }
    body
    {
        hash_t bucket_start = Integer.toInt(bucket_name, 16) << 24;

        return bucket_start >= this.toBucketStart(start) && bucket_start <= this.toBucketStart(end);
    }


    /***************************************************************************

        Tells whether a chunk file is within the specified hash range.

        Params:
            chunk_name = name of chunk file
            start = start of hash range
            end = end of hash range

    ***************************************************************************/

    private bool chunkInRange ( char[] chunk_name, hash_t start, hash_t end )
    in
    {
        assert(start <= end);
    }
    body
    {
        hash_t chunk_start = Integer.toInt(chunk_name, 16) << 12;

        return chunk_start >= this.toChunkStart(start) && chunk_start <= this.toChunkStart(end);
    }


    /***************************************************************************

        Gets the first hash in the bucket containing the given hash.

        Params:
            hash = hash to convert

    ***************************************************************************/

    private hash_t toBucketStart ( hash_t hash )
    {
        return hash & 0xFF000000;
    }


    /***************************************************************************

        Gets the last hash in the bucket containing the given hash.
    
        Params:
            hash = hash to convert
    
    ***************************************************************************/

    private hash_t toBucketEnd ( hash_t hash )
    {
        return (hash & 0xFF000000) + 0x00FFFFFF;
    }


    /***************************************************************************

        Gets the first hash in the chunk containing the given hash.
    
        Params:
            hash = hash to convert
    
    ***************************************************************************/

    private hash_t toChunkStart ( hash_t hash )
    {
        return hash & 0x00FFF000;
    }


    /***************************************************************************

        Gets the last hash in the chunk containing the given hash.
    
        Params:
            hash = hash to convert
    
    ***************************************************************************/

    private hash_t toChunkEnd ( hash_t hash )
    {
        return (hash & 0x00FFF000) + 0x0000FFFF;
    }


    /***************************************************************************

        The following code is required to fix the sizeinfo file which lives in
        channel directory. It requires that the full channel be scanned
        completely after a repair, so that the exact number of records and bytes
        in the channel can be recalculated.

        This code is disabled as it's not suitable for running on a live log
        files node, as data is continually being written to the last chunk file,
        making an accurate count of the number of records in the channel
        impossible while it is being modified.

    ***************************************************************************/

    /+
    private ulong records, bytes;

    private void checkRepair ( )
    {
        Stdout.formatln("\nChecking channel data/{}", channel);

        this.records = 0;
        this.bytes = 0;
        this.processChannel(this.channel, &this.checkBucket);

        Stdout.formatln("\nAll chunks files ok...");
    }

    private void checkBucket ( FS.FileInfo bucket, float progress )
    {
        Stdout.formatln("Checking  bucket {}{}   ({}%)", bucket.path, bucket.name, 100 * progress);
        this.processBucket(bucket, &this.checkChunk);
    }

    private char[] record_buf;
    
    private void checkChunk ( FS.FileInfo chunk, float progress )
    {
        Stdout.formatln("Checking   chunk {}{}   ({}%)", chunk.path, chunk.name, 100 * progress);

        scope file = new File(chunk.path ~ chunk.name, File.ReadExisting);
        scope ( exit ) file.close;

        auto file_length = file.length;
        auto file_pos = file.position;

        ulong record_count, byte_count;

        while ( file_pos < file_length )
        {
            hash_t key;

            SimpleSerializer.read(file, key);
            SimpleSerializer.read(file, this.record_buf);

            auto record_size = key.sizeof + size_t.sizeof + this.record_buf.length;

            file_pos += record_size;
            record_count++;
            byte_count += this.record_buf.length;
            this.records++;
            this.bytes += this.record_buf.length;
        }

        Stdout.formatln("           Chunk ok with {} records, {} bytes", record_count, byte_count);
    }


    private void updateSizeInfo ( char[] channel, ulong records, ulong bytes )
    {
        Stdout.formatln("\nUpdating sizeinfo file...");
        Stdout.formatln("{} records & {} bytes in channel", records, bytes);

        // Backup the original
        this.backupFile("data/" ~ channel ~ "/sizeinfo");

        // Write new sizeinfo
        scope file = new File;
        file.open("data/" ~ channel ~ "/sizeinfo", File.WriteExisting);
        scope ( exit ) file.close;

        SimpleSerializer.write(file, records);
        SimpleSerializer.write(file, bytes);
    }
+/
}


