/*******************************************************************************

    Memory node dump file splitter

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        December 2011: Initial release

    authors:        Gavin Norman

    Command line args:
        -S = folder to read tcm files from (multiple source folders may be
             specified)
        -D = folder to write re-split tcm files to
        -n = nodes config file

    The source folder (specified with -S) is expected to contain a series of
    one or more sub-folders each containing a set of tcm files. Each of these
    tcm files is processed in series, and its records written to new tcm files
    in the destination folder.

    The destination folder (specified with -D) is set up by the program, and
    will contain one sub-folder per destination node (as specified in the nodes
    config file, see below).

    The nodes config file (specified with -n) lists the hash ranges of the
    output nodes. These should be listed in order, as the ordering determines
    the numbering of the output folders. The file should have the following
    format:

        0x00000000 0x7fffffff
        0x80000000 0xffffffff

    (Space separated hashes, two per line, one line per node.)

*******************************************************************************/

module src.mod.tcmsplit.TcmSplitter;



/*******************************************************************************

    Imports

*******************************************************************************/

private import ocean.core.Array : copy;

private import ocean.core.Exception;

private import ocean.text.Arguments;

private import ocean.text.util.SplitIterator;

private import ocean.io.serialize.SimpleSerializer;

private import ocean.io.device.ProgressFile;

private import ocean.util.log.PeriodicTrace;

private import swarm.dht.DhtHash;

private import tango.core.BitManip : bitswap;

private import Integer = tango.text.convert.Integer;

private import Path = tango.io.Path;

private import tango.io.FilePath;

private import tango.io.device.File;

private import tango.io.stream.Buffered;

private import tango.io.Stdout;



/*******************************************************************************

    Splitter

*******************************************************************************/

public class TcmSplitter
{
    /***************************************************************************

        Source & destination folders.

    ***************************************************************************/

    private char[][] src_folders;

    private char[] dst_folder;


    /***************************************************************************

        Path of file containing the specification of the destination nodes' hash
        ranges.

    ***************************************************************************/

    private char[] nodes_file;


    /***************************************************************************

        I/O buffer size for files.

    ***************************************************************************/

    static private const IOBufferSize = 0x10000; // 64k


    /***************************************************************************

        Channel file class -- one instance of this class is created per output
        tcm file, and all are kept open and appended to as the input tcm files
        are processed. When processing of all input files has finished, the
        output files are finalized and closed.

    ***************************************************************************/

    private class ChannelFile
    {
        /***********************************************************************

            Index (in the nodes array of the outer class) of the node which this
            channel belongs to. Used for path formatting.

        ***********************************************************************/

        private size_t node_number;


        /***********************************************************************

            Name of this channel. Used for path formatting.

        ***********************************************************************/

        private char[] channel;


        /***********************************************************************

            File and outut buffer.

        ***********************************************************************/

        private File file;

        private BufferedOutput output;


        /***********************************************************************

            Count of records written to the tcm file.

        ***********************************************************************/

        private ulong records;


        /***********************************************************************

            Constructor. Opens the file and writes a dummy record count, which
            will be replaced with the true record count when the file is closed.

            Params:
                node_number = index (in this.outer.nodes array) of owning node
                channel = name of channel

        ***********************************************************************/

        public this ( size_t node_number, char[] channel )
        {
            this.node_number = node_number;
            this.channel = channel;

            this.file = new File;
            this.file.open(this.path, File.WriteCreate);
            this.output = new BufferedOutput(this.file, IOBufferSize);

            // Write dummy total number of records, to be filled in after records are written
            SimpleSerializer.write(this.output, this.records);
        }


        /***********************************************************************

            Adds a record to the channel file.

            Params:
                key = record key
                value = record value

        ***********************************************************************/

        public void put ( char[] key, char[] value )
        {
            SimpleSerializer.write(this.output, key);
            SimpleSerializer.write(this.output, value);

            this.records++;
        }


        /***********************************************************************

            Closes the channel file. The correct number of records in the file
            is written at the beginning.

        ***********************************************************************/

        public void close ( )
        {
            // Write correct number of records at the start of the file
            this.output.flush();

            this.file.seek(0);
            SimpleSerializer.write(this.file, this.records);

            this.file.close;

            Stdout.formatln("Wrote {} records to node {} '{}.tcm'", this.records,
                    this.node_number, this.channel);
        }


        /***********************************************************************

            Returns:
                the path of this channel file

        ***********************************************************************/

        private char[] path ( )
        {
            return this.outer.dst_folder ~ '/' ~ Integer.toString(this.node_number)
                ~ '/' ~ this.channel ~ ".tcm";
        }
    }


    /***************************************************************************

        Information about an output node.

    ***************************************************************************/

    static private struct Node
    {
        /***********************************************************************

            Start and end hash of this node's responsibility range.

        ***********************************************************************/

        public hash_t start;

        public hash_t end;


        /***********************************************************************

            Output tcm files for this node's channels, indexed by channel name.

        ***********************************************************************/

        public ChannelFile[char[]] channel_files;


        /***********************************************************************

            Closes all this node's channel files.

        ***********************************************************************/

        public void closeFiles ( )
        {
            foreach ( file; this.channel_files )
            {
                file.close;
            }
        }
    }


    /***************************************************************************

        Output nodes.

    ***************************************************************************/

    private Node[] nodes;


    /***************************************************************************

        File used for reading source tcm files.

    ***************************************************************************/

    private ProgressFile src_file;


    /***************************************************************************

        Constructor.

    ***************************************************************************/

    public this ( )
    {
        this.src_file = new ProgressFile(&this.progress);
    }


    /***************************************************************************

        Runs the splitting process.

        Params:
            args = arguments parsed from the command line

    ***************************************************************************/

    public void run ( Arguments args )
    {
        this.src_folders = args("source").assigned;
        this.dst_folder = args("destination").assigned[0];
        this.nodes_file = args("nodes").assigned[0];

        this.readNodesFile();

        this.setupDestination();

        this.split();
    }


    /***************************************************************************

        Reads the config file defining the output nodes.

    ***************************************************************************/

    private void readNodesFile ( )
    {
        Stdout.formatln("\n-----------------------------------------------------------------------------------");
        Stdout.formatln("Reading nodes file...");

        assertEx(Path.exists(this.nodes_file), "nodes file not found");

        // Read file content
        scope file = cast(char[])File.get(this.nodes_file);
        scope lines = new ChrSplitIterator;
        lines.delim = '\n';
        lines.reset(file);

        // Split file into lines
        scope hashes = new ChrSplitIterator;
        hashes.delim = ' ';

        foreach ( line; lines )
        {
            if ( !line.length ) continue;

            // Split line into hashes
            hashes.reset(line);

            hash_t start, end;
            bool got_end;

            foreach ( hash; hashes )
            {
                switch ( hashes.n )
                {
                    case 1:
                        start = Integer.toLong(hash);
                    break;
                    case 2:
                        end = Integer.toLong(hash);
                        got_end = true;
                    break;
                    default:
                        assertEx(false, "nodes file format invalid -- more than 2 hashes specified on a line");
                }
            }

            assertEx(got_end, "nodes file format invalid -- less than 2 hashes specified on a line");

            this.nodes ~= Node(start, end);
        }

        assertEx(this.nodes.length, "No output nodes defined");

        // Display destination nodes
        Stdout.formatln("Destination nodes:");
        foreach ( i, ref node; this.nodes )
        {
            Stdout.formatln("  {}: {:x8}..{:x8}", i, node.start, node.end);
        }
    }


    /***************************************************************************

        Creates the required destination folders.

    ***************************************************************************/

    private void setupDestination ( )
    {
        Stdout.formatln("\n-----------------------------------------------------------------------------------");
        Stdout.formatln("Setting up destination folder...");

        // Create / clear destination folder
        this.deleteFolder(this.dst_folder);
        Path.createFolder(this.dst_folder);
        assertEx(Path.exists(this.dst_folder) && Path.isFolder(this.dst_folder),
                "destination folder not found");

        // Create destination sub-folders, one per node
        foreach ( i, ref node; this.nodes )
        {
            Path.createFolder(this.dst_folder ~ '/' ~ Integer.toString(i));
        }
    }


    /***************************************************************************

        Processes the tcm files in the input folders one by one, splitting their
        contained records into the specified output folders.

    ***************************************************************************/

    private void split ( )
    {
        Stdout.formatln("\n-----------------------------------------------------------------------------------");
        Stdout.formatln("Splitting source files...");

        assertEx(Path.exists(this.dst_folder) && Path.isFolder(this.dst_folder),
                "destination folder not found");

        foreach ( src_folder; this.src_folders )
        {
            assertEx(Path.exists(src_folder) && Path.isFolder(src_folder),
                    "source folder not found");

            // Iterate over each source file in turn
            scope src = new FilePath(src_folder);
            foreach ( child; src )
            {
                auto child_path = child.path ~ child.name;
    
                if ( child.folder )
                {
                    this.splitFolder(child_path);
                }
                else
                {
                    Stderr.formatln("Ignoring file in source folder: {}", child_path);
                }
            }
        }

        foreach ( ref node; this.nodes )
        {
            node.closeFiles;
        }
    }


    /***************************************************************************

        Recursively deletes a folder and all its contents.

    ***************************************************************************/

    private void deleteFolder ( char[] path )
    {
        if ( !Path.exists(path) || !Path.isFolder(path) )
        {
            return;
        }

        // Remove files in folder and recurse into sub-folders
        scope folder = new FilePath(path);
        foreach ( child; folder )
        {
            auto child_path = child.path ~ child.name;

            if ( child.folder )
            {
                this.deleteFolder(child_path);
            }
            else
            {
                Path.remove(child_path);
                Stdout.formatln("Removed file {}", child_path);
            }
        }

        // Remove folder
        Path.remove(path);
        Stdout.formatln("Removed folder {}", path);
    }


    /***************************************************************************

        Splits all tcm files found within the specified folder.

        Params:
            path = path of folder containing tcm files to split

    ***************************************************************************/

    private void splitFolder ( char[] path )
    {
        scope folder = new FilePath(path);
        foreach ( child; folder )
        {
            auto child_path = child.path ~ child.name;

            if ( !child.folder )
            {
                char[] channel;
                if ( this.isChannel(child.name, channel) )
                {
                    this.splitFile(child_path, channel);
                }
                else
                {
                    Stderr.formatln("Ignoring non-tcm file in source folder: {}", child_path);
                }
            }
            else
            {
                Stderr.formatln("Ignoring sub-folder in source folder: {}", child_path);
            }
        }
    }


    /***************************************************************************

        Input file progress delegate. Displays file scanning progress on the
        console.

    ***************************************************************************/

    private void progress ( size_t bytes, ulong total_bytes )
    {
        auto pcnt = (cast(float)total_bytes / cast(float)this.src_file.length) * 100.0;
        StaticPeriodicTrace.format("{}: {}%", this.src_file, pcnt);
    }


    /***************************************************************************

        Splits the specified tcm file, copying all records to the specified
        channel in the output nodes.

        Params:
            path = path of tcm file to split
            channel = channel to send records to

    ***************************************************************************/

    private void splitFile ( char[] path, char[] channel )
    {
        Stdout.formatln("Processing channel file {}", path);

        this.src_file.open(path, ProgressFile.ReadExisting);

        this.src_file.open(path, ProgressFile.ReadExisting);
        scope ( exit ) this.src_file.close;

        scope input = new BufferedInput(this.src_file, IOBufferSize);

        // Read number of records
        ulong num_records;
        SimpleSerializer.read(input, num_records);

        // Read records
        ulong i;
        char[] key, value;
        for ( i = 0; i < num_records; i++ )
        {
            SimpleSerializer.read(input, key);
            SimpleSerializer.read(input, value);
            this.handleRecord(channel, key, value);
        }

        Stdout.formatln("  Extracted {} records                                                  ", i);
    }


    /***************************************************************************

        Writes a record to the appropriate output file. If no output node is
        found to be responsible for the record's key, an warning is printed to
        the console.

        Params:
            channel = record's channel
            key = record's key (8 hex digits)
            value = record's value

    ***************************************************************************/

    private void handleRecord ( char[] channel, char[] key, char[] value )
    {
        auto hash = bitswap(DhtHash.straightToHash(key));
        size_t i;
        if ( this.nodeIndex(hash, i) )
        {
            auto file = channel in this.nodes[i].channel_files;
            if ( file is null )
            {
                this.nodes[i].channel_files[channel] = new ChannelFile(i, channel);
                file = channel in this.nodes[i].channel_files;
                assert(file !is null);
            }
    
            file.put(key, value);
        }
        else
        {
            Stderr.formatln("No output node for record! {}:{}:{}", channel, key, value);
        }
    }


    /***************************************************************************

        Gets the index of the node (in the this.nodes array) which is
        responsible for the given hash.

        Params:
            hash = hash to find responsible node for
            index = output value, receives index of responsible node in
                this.nodes

        Returns:
            true if responsible node was found

    ***************************************************************************/

    private bool nodeIndex ( hash_t hash, out size_t index )
    {
        foreach ( i, ref node; this.nodes )
        {
            if ( hash >= node.start && hash <= node.end )
            {
                index = i;
                return true;
            }
        }

        return false;
    }


    /***************************************************************************

        Tells if the given filename is a dumped memory channel file, and, if so,
        extracts the name of the channel it contains.

        Params:
            filename = name of file to check
            channel = receives the name of the channel in the file, if the file
                is a dumped channel

        Returns:
            true if the file is a dumped memory channel

    ***************************************************************************/

    private bool isChannel ( char[] filename, ref char[] channel )
    {
        const ending = ".tcm";

        if ( filename.length < ending.length || filename[$-ending.length .. $] != ending )
        {
            return false;
        }

        channel.copy(filename[0 .. $-ending.length]);
        return true;
    }
}

