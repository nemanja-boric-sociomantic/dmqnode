/******************************************************************************

    Output implementation for Distributed Hashtable LogFiles storage engine

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        August 2010: Initial release
                    January 2011: Asynchronous version

    authors:        David Eckardt, Gavin Norman

    Implements a set of static methods to read records from a log file storage
    engine.

    Database file organization

    The base directory contains numbered subdirectories, referred to as "slot
    directories" or "slots". Each slot again contains numbered files, referred
    to as "bucket files" or "buckets". Each bucket file contains a sequence of
    records where each record consists of a header, which contains the record
    key and the value data byte length, followed by the value data. The file
    structure looks like this:

    base_dir/
        slot1/
            bucket1
            bucket2
            ...
        slot2/
            bucket1
            bucket2
            ...
        ...

    The bucket which contains a record and the slot which contains that bucket
    are determined from the record key as follows.

    The key type is an unsigned integer type, usually 32 bits in length,
    corresponding to a binary number of 32 digits which is identical to an 8
    digit hexadecimal number.

    A range of keys where only the last 3 hexadecimal digits vary is associated
    to a single bucket. That means that a bucket corresponds to a sequence of
    4096 keys.

    Of the remaining 5 hexadecimal digits the rounded down first half, which
    consists of the first 2 hexadecimal digits, denotes the slot.
    The remaining 3 hexadecimal digits of the key denote the bucket in the slot.

    Examples:
    1. Key 0x12345678 is located in bucket 345 in slot 12.
    2. The keys in the range from 0x12345000 to 0x12345FFF are located in bucket
       345 in slot 12.
    3. The keys in the range from 0x12340000 to 0x12348FFF are located in
       buckets 340, 341, 342..., 348 in slot 12.
    4. The keys in the range from 0x10000000 to 0x12348FFF are located in
        - buckets 000 to FFF in slot 10 and 11,
        - buckets 000 to 348 in slot 12.

    As a result of the slot/bucket association method for a key width of 32 bits
    the base directory contains up to 255 slot directories and each slot
    directory contains up to 4096 bucket files.

    Each slot directory and bucket file is created on the first write request
    with a record key associated to them.

 ******************************************************************************/

module src.mod.dht.storage.logfiles.LogRecord;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.dht.DhtHash;

private import src.mod.dht.storage.logfiles.SizeInfoFile;

private import ocean.core.Array;
private import ocean.core.Exception: assertEx;

private import ocean.io.serialize.SimpleSerializer;

debug private import ocean.util.log.Trace;

private import ocean.text.convert.Layout;

private import tango.io.model.IConduit: IOStream, InputStream, OutputStream;

private import tango.io.device.File;

private import tango.io.FilePath;

private import tango.core.Exception: IOException;

private import Integer = tango.text.convert.Integer : toUlong;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("src.mod.dht.storage.logfiles.LogRecord");
}



public class LogRecord
{
    /**************************************************************************

        Private constructor to prevent instantiation.

     **************************************************************************/

    private this ( ) { }

    /**************************************************************************

        Everything else is static.

     **************************************************************************/

    static:

    /**************************************************************************

        Definition of the range of hexadecimal and binary digits of a key which
        correspond to the slot and bucket and the remaining key digits.
        See description at the top.

     **************************************************************************/

    public const struct SplitBits
    {
        static const uint total_digits  = hash_t.sizeof * 2,
                          key_digits    = 3,            // 4095 keys per bucket
                          bucket_digits = 3,            // 4095 buckets per slot
                          slot_digits   = total_digits -
                                          (key_digits + bucket_digits),

                          key_bits      = key_digits * 4,
                          bucket_bits   = bucket_digits * 4,
                          slot_bits     = slot_digits * 4;

//        pragma(msg, "total hex digits = " ~ total_digits.stringof);
//        pragma(msg, "slot digits = " ~ slot_digits.stringof);
//        pragma(msg, "bucket digits = " ~ bucket_digits.stringof);
//        pragma(msg, "key digits = " ~ key_digits.stringof);
    }

    /**************************************************************************

        Record header struct definition; contains the key and the value length
        of a record

     **************************************************************************/

    public align (1) struct RecordHeader
    {
        hash_t key;
        size_t len;
    }

    /**************************************************************************

        Gets the filename of the first bucket file in the given base directory.

        Params:
            base_dir = base directory
            path = string into which the filename of the first bucket file will
                be written
            bucket_start = if a bucket file is found, receives the value of the
                first (theoretical) hash in that bucket

        Returns:
            true if no bucket files exist in the base dir or sub-directories
            (i.e. the storage channel is empty)

     **************************************************************************/

    public bool getFirstBucket ( char[] base_dir, ref char[] path,
        out hash_t bucket_start )
    {
        return getFirstBucketInRange(base_dir, path, bucket_start, hash_t.min,
            hash_t.max);
    }

    /**************************************************************************

        Gets the filename of the first bucket file in the given base directory
        and within the specified hash range.

        Params:
            base_dir = base directory
            path = string into which the filename of the first bucket file will
                be written
            bucket_start = if a bucket file is found, receives the value of the
                first (theoretical) hash in that bucket
            min_hash = start of search range
            max_hash = end of search range

        Returns:
            true if no bucket files exist in the base dir or sub-directories
            within the specified hash range

     **************************************************************************/

    public bool getFirstBucketInRange ( char[] base_dir, ref char[] path,
        out hash_t bucket_start, hash_t min_hash, hash_t max_hash )
    {
        // Remove key number from min & max hashes (so just the slot/bucket
        // numbers remain)
        hash_t min_bucket_slot = min_hash >> SplitBits.key_bits;
        hash_t max_bucket_slot = max_hash >> SplitBits.key_bits;

        hash_t next_bucket_slot;
        auto empty = getFirstBucket_(base_dir, path, next_bucket_slot,
            min_bucket_slot, max_bucket_slot);
        if ( !empty )
        {
            if ( next_bucket_slot >= min_bucket_slot && next_bucket_slot <= max_bucket_slot )
            {
                bucket_start = next_bucket_slot << SplitBits.key_bits;
            }
            else
            {
                empty = true;
            }
        }

        return empty;
    }

    /**************************************************************************

        Gets the filename of the next bucket file in the given base directory
        after the specified last hash, and up to the specified maximum hash. The
        first hash of the new bucket is also returned via the ref hash
        parameter.

        Params:
            base_dir = base directory
            path = string into which the filename of the first bucket file will
                be written
            bucket_start = if a bucket file is found, receives the value of the
                first (theoretical) hash in that bucket (hash_t.min otherwise)
            last_hash = a hash in the previous bucket, used to calculate the
                hash range of the next bucket
            max_hash = highest hash bucket to return

        Returns:
            true if no bucket files exist in the base dir or sub-directories
            within the specified hash range

     **************************************************************************/

    public bool getNextBucket ( char[] base_dir,
        ref char[] path, out hash_t bucket_start,
        hash_t last_hash, hash_t max_hash = hash_t.max )
    {
        hash_t min_bucket_slot = last_hash >> SplitBits.key_bits;
        hash_t max_bucket_slot = max_hash >> SplitBits.key_bits;

        // If this is already the last bucket, then there can be no more
        if ( min_bucket_slot >= max_bucket_slot )
        {
            return true;
        }

        // Otherwise look for the next existing bucket file.
        min_bucket_slot++;
        hash_t next_bucket_slot;
        auto no_bucket = getFirstBucket_(base_dir, path, next_bucket_slot,
            min_bucket_slot, max_bucket_slot);

        if ( !no_bucket )
        {
            // set to first hash in bucket
            bucket_start = next_bucket_slot << SplitBits.key_bits;
        }

        return no_bucket;
    }

    /***************************************************************************

        Reads the header of a record from the current seek position of an open
        bucket file. The seek position is moved ready to read the record's
        value.

        Params:
            file = currently open bucket file
            header = (output) header of next record

        Returns:
            true if the end of the bucket file was reached

    ***************************************************************************/

    public bool nextRecord ( File file, ref RecordHeader header )
    {
        auto file_length = file.length;
        auto file_pos = file.position;

        size_t bytes_read;

        if ( file_pos >= file_length )
        {
            return true; // end of bucket file
        }

        // Read header of next record
        bytes_read = SimpleSerializer.readData(file, &header, header.sizeof);
        assertEx(bytes_read == header.sizeof, typeof(this).stringof ~ "readRecord - header read failed");
        file_pos += bytes_read;

        // Sanity check: if the length of the record is beyond the end of
        // the file, then just return. This can occur in two cases:
        //      1. A record being read as it is being written
        //      2. Corrupt data
        if ( file_pos + header.len > file_length )
        {
            return true; // end of bucket file
        }

        return false; // read header successfully
    }

    /***************************************************************************

        Reads the value of a record from the current seek position of an open
        bucket file. The seek position is moved ready to read the next record's
        header.

        Params:
            file = currently open bucket file
            header = header of current record
            value = (output) receives record value

    ***************************************************************************/

    public void readRecordValue ( File file, RecordHeader header, ref char[] value )
    {
        // Read value from file
        value.length = header.len;
        auto bytes_read = SimpleSerializer.readData(file, value.ptr,
            value.length * typeof(value[0]).sizeof);
        assertEx(bytes_read == value.length * typeof(value[0]).sizeof, typeof(this).stringof ~ "readRecord - record read failed");
    }

    /***************************************************************************

        Skips over the value of a record from the current seek position of an
        open bucket file, without actually reading the value from the file. The
        seek position is moved ready to read the next record's header.

        Params:
            file = currently open bucket file
            header = header of current record

    ***************************************************************************/

    public void skipRecordValue ( File file, RecordHeader header )
    {
        file.seek(header.len, File.Anchor.Current);
    }

    /**************************************************************************

        Removes all bucket files and slot directories found within the specified
        base directory.

        Params:
            base_dir = base directory to scan for slot dirs

     **************************************************************************/

    public void removeFiles ( char[] base_dir )
    {
        scope dir_path  = new FilePath(base_dir);
        scope slot_path = new FilePath;
        scope file_path = new FilePath;

        foreach ( dir_info; dir_path )
        {
            slot_path.file = dir_info.name;
            slot_path.folder = dir_info.path;

            if ( slot_path.isFolder )
            {
                char[SplitBits.bucket_digits] first_bucket = 'Z';
                foreach ( file_info; slot_path )
                {
                    file_path.file = file_info.name;
                    file_path.folder = file_info.path;

                    file_path.remove();
                }
            }

            slot_path.remove();
        }
    }

    /***************************************************************************

        Helper class used by getFirstBucket_() to work around the fact that:
        ---
            void f ( int a ) { const b = a + 12; }
        ---
        causes a compile error in D1.

    ***************************************************************************/

    private static scope class Const ( T )
    {
        public const T val;

        public this ( T val )
        {
            this.val = val;
        }
    }

    /***************************************************************************

        Gets the filename of the first bucket file in the given base directory
        and within the given bucket number. First the slot folders in the base
        directory are scanned for the lowest matching slot (within the specified
        range). If a suitable slot folder is found, its bucket files are scanned
        for the lowest matching bucket (again within the specified range). This
        process is reapeated until either a matching slot/bucket is found, or
        all slots/buckets have been scanned and none were within the specified
        range.

        Params:
            base_dir = base directory containing slot folders
            path = string into which the filename of the first bucket file found
                will be written (unchanged if none found)
            found_bucket_slot = receives value of matching slot/bucket on
                success (hash_t.min otherwise) *
            min_bucket_slot = value of minimum slot/bucket allowed *
            max_bucket_slot = value of maximum slot/bucket allowed *

        * The slot/bucket values are specified as hashes where the lowest
          SplitBits.bucket_bits contain the bucket value, and the following
          SplitBits.slot_bits contain the slot.

        Returns:
            true if no bucket files exist in the base dir's sub-directories
            within the specified range

    ***************************************************************************/

    private bool getFirstBucket_ ( char[] base_dir,
        ref char[] path, out hash_t found_bucket_slot,
        hash_t min_bucket_slot, hash_t max_bucket_slot )
    {
        scope min_slot = new Const!(hash_t)
            (min_bucket_slot >> SplitBits.bucket_bits);
        scope max_slot = new Const!(hash_t)
            (max_bucket_slot >> SplitBits.bucket_bits);
        hash_t slot = min_slot.val;

        bool no_bucket;
        do
        {
            // Find first slot directory within range
            hash_t found_slot;
            auto no_slot = findFirstSlotDirectory(base_dir, found_slot,
                slot, max_slot.val);
            if ( no_slot )
            {
                return true;
            }
            slot = found_slot;

            // Work out which buckets within the slot directory count as a
            // match. Generally, all buckets in a slot are valid...
            const hash_t bucket_mask = (1 << SplitBits.bucket_bits) - 1;
            hash_t min_bucket = hash_t.min;  // 000
            hash_t max_bucket = bucket_mask; // fff

            // ...but if this is the first slot in the range, respect the min
            // bucket specified...
            if ( slot == min_slot.val )
            {
                min_bucket = min_bucket_slot & bucket_mask;
            }
            // ...or if this is the last slot in the range, respect the max
            // bucket specified.
            else if ( slot == max_slot.val )
            {
                max_bucket = max_bucket_slot & bucket_mask;
            }

            // Find first bucket file in slot directory within range
            char[SplitBits.slot_digits] slot_name_buf;
            auto slot_name = DhtHash.intToHex(slot, slot_name_buf);

            hash_t found_bucket;
            no_bucket = findFirstBucketFile(base_dir, slot_name, found_bucket,
                min_bucket, max_bucket);
            if ( !no_bucket )
            {
                // Set output parameters
                char[SplitBits.bucket_digits] bucket_name_buf;
                auto bucket_name = DhtHash.intToHex(found_bucket, bucket_name_buf);

                path.length = 0;
                Layout!(char).print(path, "{}/{}/{}", base_dir, slot_name,
                    bucket_name);

                found_bucket_slot = (slot << SplitBits.bucket_bits) +
                    found_bucket;

                return false;
            }

            // Try again starting at the next slot
            slot++;
        }
        while ( slot < max_slot.val );

        return true;
    }

    /***************************************************************************

        Scans the given base directory for the lowest matching slot folder
        within the specified range. If a suitable match is found, its value is
        returned via the ref min_slot argument.

        Params:
            base_dir = base directory containing slot folders
            found_slot = receives value of matching slot on success
                (hash_t.min otherwise) *
            min_slot = value of minimum slot allowed *
            max_slot = value of maximum slot allowed *

        * The slot values are specified as hashes where the lowest
          SplitBits.slot_bits contain the slot value.

        Returns:
            true if no slot folder exists in the base dir within the specified
            range

    ***************************************************************************/

    private bool findFirstSlotDirectory ( char[] base_dir, out hash_t found_slot,
        hash_t min_slot, hash_t max_slot )
    {
        scope scan_path = new FilePath();
        scan_path.folder = base_dir;
        scan_path.file = "";

        hash_t first_slot;
        auto no_slot = findLowestSubPath(scan_path, true, first_slot, min_slot);
        if ( no_slot )
        {
            return true;
        }

        found_slot = first_slot;
        return false;
    }

    /***************************************************************************

        Scans the given slot directory (inside the base directory) for bucket
        files starting at the specified minimum value. The existence of every
        possible bucket file is checked until either one is discovered or all
        have been checked and none has been found.

        Note: this scanning algorithm is used, instead of an algorithm which
        iterates over all extant files in the slot folder, in order to avoid
        having to scan all (unordered) files every time the method is called.
        As it is, usually bucket files with all values will exist (in most
        slot folders), so it is much more efficient to simply check for the
        existence of the next file.

        Params:
            base_dir = base directory containing slot folders
            slot_dir = slot folder to scan
            found_bucket = receives value of matching bucket on success
                (hash_t.min otherwise) *
            min_bucket = value of minimum bucket allowed *
            max_bucket = value of maximum bucket allowed *

        * The bucket values are specified as hashes where the lowest
          SplitBits.bucket_bits contain the bucket value.

        Returns:
            true if no bucket file exists in the slot folder within the
            specified range

    ***************************************************************************/

    private bool findFirstBucketFile ( char[] base_dir, char[] slot_dir,
        out hash_t found_bucket, hash_t min_bucket, hash_t max_bucket )
    {
        scope scan_path = new FilePath;
        scan_path.folder = slot_dir;
        scan_path.prepend(base_dir);

        while ( min_bucket <= max_bucket )
        {
            char[SplitBits.bucket_digits] bucket_name_buf;

            scan_path.file = DhtHash.intToHex(min_bucket, bucket_name_buf);

            // Check whether bucket file exists
            if ( scan_path.exists )
            {
                found_bucket = min_bucket;
                return false;
            }

            // Otherwise try the next bucket file.
            min_bucket++;
        }

        return true;
    }

    /***************************************************************************

        Iterates over all children (either child folders or child files) of the
        given path in order to find the child with the lowest hexadecimal
        filename value (i.e. the child's filename simply converted to an integer
        - filenames containing non-hex characters are ignored). Optionally a
        minimum integer value can be specified, in order to filter out children
        whose filename is < the specified minimum.

        Params:
            path = file path to search within
            folders = flag indicating whether child folders (true) or child
                files (false) should be scanned
            lowest = receives the value of the lowest matching file / folder
                (or hash_t.max if no match is found)
            min = optional minimum value to scan for

        Returns:
            true if no matching folder/file exists

    ***************************************************************************/

    private bool findLowestSubPath ( FilePath path, bool folders,
        out hash_t lowest, hash_t min = hash_t.min )
    {
        bool found;
        lowest = hash_t.max;

        // Find file in directory with 'lowest' name
        foreach ( child; path )
        {
            // Check that child is of the expected type.
            if ( child.folder != folders )
            {
                // Don't log a warning in the case of the sizeinfo file, which
                // is expected in a channel's base directory.
                if ( !(folders && child.name == SizeInfoFile.FileName) )
                {
                    log.warn("{} '{}' found when expecting only {}",
                        folders ? "File" : "Folder", child.name,
                        folders ? "folders" : "files");
                }

                continue;
            }

            // Get integer value of child's name (handling errors)
            hash_t value;
            try
            {
                value = Integer.toUlong(child.name, 16);
            }
            catch ( Exception e )
            {
                log.warn("{} found with invalid name: {} ('{}')",
                    folders ? "Folder" : "File", child.name, e.msg);
                continue;
            }

            // Ignore child if value is less than the specified minimum.
            if ( value < min )
            {
                continue;
            }

            // Update lowest value counter, if child's value is lower.
            if ( !found || value < lowest )
            {
                found =  true;
                lowest = value;
            }
        }

        return !found;
    }
}

