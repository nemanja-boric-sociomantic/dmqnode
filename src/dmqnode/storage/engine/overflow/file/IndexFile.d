/*******************************************************************************

    The index file of the queue disk overflow.

    This is a text file where each line corresponds to one queue channel and
    contains the channel name as the first token, followed by the decimal values
    of the following ChannelMetadata fields in that order: records, bytes,
    first_offset, last_offset. Tokens are separated by whitespace.
    The numeric channel ID is not stored in the index file. After reading the
    index file it read from the first or last record in the data file.

    The index file is a text file for the sake of easy inspection. It should be
    written only by the IndexFile.writeLines() method.

    copyright: Copyright (c) 2016 sociomantic labs. All rights reserved

*******************************************************************************/

module dmqnode.storage.engine.overflow.file.IndexFile;

import dmqnode.storage.engine.overflow.ChannelMetadata;
import dmqnode.storage.engine.overflow.file.PosixFile;

class IndexFile: PosixFile
{
    import ocean.core.Enforce: enforceImpl;
    import ocean.stdc.posix.signal: SIGABRT, SIGSEGV, SIGILL, SIGBUS;
    import ocean.stdc.posix.stdio: fdopen;
    import ocean.stdc.stdio: FILE, EOF, fscanf, fprintf, feof, rewind, clearerr, fflush;
    import ocean.stdc.stdlib: free;
    import ocean.sys.SignalMask;

    /***************************************************************************

        Signals that should not be blocked because the program should be
        terminated immediately if one of these is raised.

    ***************************************************************************/

    public static const signals_dontblock = [SIGABRT, SIGSEGV, SIGBUS, SIGILL];

    /***************************************************************************

        Signal set to block all signals except unblocked_signals while
        formatted file I/O functions are running, which cannot be restarted or
        recovered if interrupted by a signal.

    ***************************************************************************/

    private static SignalSet fmt_io_signal_blocker;

    static this ( )
    {
        this.fmt_io_signal_blocker = this.fmt_io_signal_blocker; // Pacify compiler
        this.fmt_io_signal_blocker.setAll();
        this.fmt_io_signal_blocker.remove(this.signals_dontblock);
    }

    /***************************************************************************

        The file as stdio FILE stream.

    ***************************************************************************/

    public FILE* stream;

    /**************************************************************************/

    invariant ( )
    {
        assert(this.stream);
    }

    /***************************************************************************

        Constructor.

        Params:
            dir  = working directory
            name = file name

        Throws:
            FileException on file I/O error.

    ***************************************************************************/

    public this ( char[] dir, char[] name )
    {
        super(dir, name);
        this.stream = fdopen(this.fd, "w+".ptr);
        this.enforce(this.stream, "unable to fdopen");
    }

    /***************************************************************************

        Parses the index file and calls got_channel for each channel in the
        file. The channel_name and channel arguments passed to got_channel are
        validated: channel_name is a valid queue channel name, and channel is
        validated according to the criteria of its invariant.

        Params:
            got_channel = called for each channel with validated channel_name
                          and channel; nline is the line number

        Throws:
            FileException on file I/O error or bad index file content (parse
            error or values that would make the ChannelMetadata invariant fail).

    ***************************************************************************/

    public void readLines ( void delegate ( char[] channel_name,
                                            ChannelMetadata channel,
                                            uint nline ) got_channel )
    {
        rewind(this.stream);

        for (uint nline = 1;; nline++)
        {
            ChannelMetadata channel;
            int name_start, name_end;
            char* channel_name = null;

            scope (exit)
            {
                /*
                 * fscanf() allocates channel_name via malloc() on a match or
                 * leaves it untouched (null) on mismatch.
                 */
                if (channel_name) free(channel_name);
            }

            int n;
            this.fmt_io_signal_blocker.callBlocked(
                /*
                 * Special fscanf format tokens:
                 *   - The leading ' ' skips leading white space.
                 *   - %n stores the current position in the input string in the
                 *     argument so that
                 *     channel_name.length = name_end - name_start.
                 *   - %m matches a string, stores it in a buffer allocated by
                 *     malloc and stores a pointer to that buffer in the
                 *     argument.
                 *   - [_0-9a-zA-Z-] makes %m match only strings that consist of
                 *     the characters '_', '0'-'9', 'a'-'z', 'A'-'Z' or '-',
                 *     which ensures the string is a valid queue channel name.
                 */
                n = fscanf(this.stream,
                           " %n%m[_0-9a-zA-Z-]%n %lu %llu %lld %lld".ptr,
                           &name_start, &channel_name, &name_end,
                           &channel.records, &channel.bytes, &channel.first_offset,
                           &channel.last_offset)
            );

            switch (n)
            {
                case 5:
                    /*
                     * Validate channel by checking the same conditions as its
                     * invariant.
                     */
                    channel.validate(channel,
                        (bool good, char[] msg)
                        {
                            enforceImpl(this.e, good, msg, this.name, nline);
                        });

                    got_channel(channel_name[0 .. name_end - name_start], channel, nline);
                    break;

                case EOF:
                    this.enforce(feof(this.stream), "Error reading channel index",
                                 "feof", this.name, nline);
                    return;

                default:
                    this.enforce(!feof(this.stream), "Unexpected end of file",
                                 "feof", this.name, nline);
                    static const char[][] errmsg =
                    [
                        "Invalid channel name"[],
                        "Invalid number of records",
                        "Invalid number of bytes",
                        "Invalid position of first record",
                        "Invalid offset of last record"
                    ];
                    this.e.msg = errmsg[n];
                    this.e.file = this.name;
                    this.e.line = nline;
                    throw this.e;
            }
        }
    }

    /***************************************************************************

        Resets the index file to be empty, then writes lines to the index file.

        Calls iterate, which in turn should call writeln for each line that
        should be written to the index file. All signals except the ones in
        this.signals_dontblock are blocked while iterate is executing. Flushes
        the index file output after iterate has returned (not if it throws).

        Params:
            iterate = called once with a writeln delegate as argument; each call
                      of writeln writes one line to the index file

        Throws:
            FileException on file I/O error.

    ***************************************************************************/

    public void writeLines ( void delegate ( void delegate ( char[] name, ChannelMetadata channel ) writeln ) iterate )
    {
        this.reset();

        this.fmt_io_signal_blocker.callBlocked({
            iterate((char[] name, ChannelMetadata channel)
            {
                int n = fprintf(this.stream, "%.*s %lu %llu %lld %lld\n".ptr,
                                name.length, name.ptr,
                                channel.records, channel.bytes,
                                channel.first_offset, channel.last_offset);

                this.enforce(n >= 0, "error writing index");
            });
            this.enforce(!fflush(this.stream), "error flushing index");
        }());
    }

    /***************************************************************************

        Resets the error indicator when the file is truncated to be empty.

    ***************************************************************************/

    override public void reset ( )
    {
        super.reset();
        clearerr(this.stream);
    }
}
