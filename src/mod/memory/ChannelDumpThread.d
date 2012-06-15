/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        05/06/2012: Initial release

    authors:        Gavin Norman

    Thread which runs alongside the server thread and periodically triggers the
    memory storage channels to dump to disk.

*******************************************************************************/

module src.mod.memory.ChannelDumpThread;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.dht.node.storage.MemoryStorageChannels;

private import src.core.util.Terminator;

private import tango.core.Thread;

private import ocean.sys.SignalMask;
private import tango.stdc.signal: SIGABRT, SIGINT, SIGTERM;

private import tango.time.StopWatch;

private import tango.util.log.Log;

private import tango.math.random.Random;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("src.mod.node.memory.ChannelDumpThread");
}



/*******************************************************************************

    Informational interface to the channel dump thread.

*******************************************************************************/

public interface IChannelDumpInfo
{
    /***************************************************************************

        Returns:
            true if a dump is in progress

    ***************************************************************************/

    public bool busy ( );


    /***************************************************************************

        Returns:
            the number of seconds until the next dump (0 if a dump is in
            progress)

    ***************************************************************************/

    public uint seconds_until_dump ( );
}



/*******************************************************************************

    Memory storage channel dumping thread class.

*******************************************************************************/

public class ChannelDumpThread : Thread, IChannelDumpInfo
{
    /***************************************************************************

        Reference to the memory storage channels to be dumped.

    ***************************************************************************/

    private const MemoryStorageChannels storage_channels;


    /***************************************************************************

        Seconds delay between dumps. The actual delay after a dump has finished
        is calculated as: dump_period - s, where s is the number of seconds the
        last dump took to finish.

    ***************************************************************************/

    private const uint dump_period;


    /***************************************************************************

        Minimum time to wait bewteen dumps. The thread will always wait for at
        least this number of seconds between dumps, irrespective of what the
        value of dump_period - s (see above) is.

    ***************************************************************************/

    private const min_wait_time = 60.0;


    /***************************************************************************

        Current time remaining to wait - updated in the thread's method
        (threadRun).

    ***************************************************************************/

    private float wait_time;


    /***************************************************************************

        Flag set to true when a dump is in progress.

    ***************************************************************************/

    private bool busy_;


    /***************************************************************************

        Constructor. Starts the dump thread running.

        Params:
            storage_channels = storage channels to dump
            dump_period = seconds between channel dumps

    ***************************************************************************/

    public this ( MemoryStorageChannels storage_channels, uint dump_period )
    {
        this.storage_channels = storage_channels;
        this.dump_period = dump_period;

        super(&this.threadRun);
    }


    /***************************************************************************

        Returns:
            true if a dump is in progress, false if thread is waiting

    ***************************************************************************/

    public bool busy ( )
    {
        return this.busy_;
    }


    /***************************************************************************

        Returns:
            the number of seconds until the next dump (0 if a dump is in
            progress)

    ***************************************************************************/

    public uint seconds_until_dump ( )
    {
        return this.busy_ ? 0 : cast(uint)this.wait_time;
    }


    /***************************************************************************

        Thread run method. Repeatedly waits dump_period then dumps the channels.
        The termination flag in mod.node.util.Terminator causes the thread to
        exit immediately (if waiting) or after the current dump has finished (if
        a dump is in progress).

    ***************************************************************************/

    private void threadRun ( )
    {
        // Mask signals which must be handled by the main thread.
        maskSignals([SIGABRT, SIGINT, SIGTERM]);

        log.trace("ChannelDumpThread started");
        scope ( exit ) log.trace("ChannelDumpThread exiting");

        // Set initial wait time randomly (up to dump_period), to ensure that
        // all nodes are not dumping simultaneously.
        scope rand = new Random;
        uint random_wait_time;
        rand(random_wait_time);
        this.wait_time = random_wait_time % this.dump_period;

        log.info("Performing initial dump in {}s (randomized)", this.wait_time);

        const sleep_time = 0.25;

        do
        {
            Thread.sleep(sleep_time);

            if ( !Terminator.terminating )
            {
                this.wait_time -= sleep_time;
                if ( this.wait_time <= 0.0 )
                {
                    log.info("Starting channel dump");

                    StopWatch sw;
                    sw.start;

                    this.busy_ = true;

                    try
                    {
                        this.storage_channels.maintenance();
                    }
                    catch ( Exception e )
                    {
                        log.error("Error while dumping: {} @ {}:{}", e.msg,
                            e.file, e.line);
                    }

                    this.finishedDump(sw.microsec);
                    this.busy_ = false;
                }
            }
        }
        while ( !Terminator.terminating );
    }


    /***************************************************************************

        Called when a dump has finished. Calculates the wait time until the next
        dump should begin.

        Params:
            us_taken = microseconds taken by the last dump

    ***************************************************************************/

    private void finishedDump ( ulong us_taken )
    {
        auto s_taken = cast(float)us_taken / 1_000_000.0;
        this.wait_time = cast(float)this.dump_period - s_taken;

        log.info("Finished channel dump in {}s", s_taken);

        if ( this.wait_time < this.min_wait_time )
        {
            log.warn("Calculated wait time too short -- either the "
                "channel dump took an unusually long time, or the "
                "dump period is set too low in config.ini.");
            this.wait_time = this.min_wait_time;
        }

        log.info("Dumping again in {}s", this.wait_time);
    }
}

