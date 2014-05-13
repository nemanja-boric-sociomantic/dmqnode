/*******************************************************************************

    Copyright:      Copyright (c) 2014 sociomantic labs. All rights reserved

    Dht node channel dump tool.

*******************************************************************************/

module swarmnodes.dht.memory.dhtdump.main;



/*******************************************************************************

    Imports

*******************************************************************************/

private import Version;

private import swarmnodes.dht.memory.storage.DumpFile;

private import ocean.core.Array : appendCopy, copy;

private import ocean.io.FilePath;

private import ocean.io.select.EpollSelectDispatcher;

private import ocean.text.convert.Integer : toUshort;

private import ocean.util.app.VersionedLoggedStatsCliApp;

private import ConfigReader = ocean.util.config.ClassFiller;

private import swarm.dht.DhtClient;

private import tango.core.Array : contains;
private import tango.core.Thread;

private import tango.math.random.Random;

private import tango.time.StopWatch;

private import tango.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;

static this ( )
{
    log = Log.lookup("swarmnodes.dhtdump.DhtDump");
}



/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts dhtdump.

    Params:
        cl_args = array with raw command line arguments

*******************************************************************************/

private int main ( char[][] cl_args )
{
    auto app = new DhtDump;
    return app.main(cl_args);
}



public class DhtDump : VersionedLoggedStatsCliApp
{
    /***************************************************************************

        Epoll selector instance

    ***************************************************************************/

    private const EpollSelectDispatcher epoll;


    /***************************************************************************

        Dht client instance

    ***************************************************************************/

    private const DhtClient dht;


    /***************************************************************************

        List of channels being iterated over

    ***************************************************************************/

    private char[][] channels;


    /***************************************************************************

        Path to write dump files to

    ***************************************************************************/

    private FilePath root;


    /***************************************************************************

        Path of current dump file

    ***************************************************************************/

    private FilePath path;


    /***************************************************************************

        Path of file being swapped (see swapNewAndBackupDumps())

    ***************************************************************************/

    private FilePath swap_path;


    /***************************************************************************

        Dump file

    ***************************************************************************/

    private ChannelDumper file;


    /***********************************************************************

        Path of temporary file being dumped to.

    ***********************************************************************/

    private char[] dump_path;


    /***************************************************************************

        Dht settings, read from config file

    ***************************************************************************/

    private static class DhtConfig
    {
        char[] address;
        ushort port;
    }

    private DhtConfig dht_config;


    /***************************************************************************

        Dump settings, read from config file

    ***************************************************************************/

    private static class DumpConfig
    {
        char[] data_dir = "data";
        uint period_s = 60 * 60 * 4;
        uint min_wait_s = 60;
        uint retry_wait_s = 30;
    }

    private DumpConfig dump_config;


    /***************************************************************************

        Constructor.

    ***************************************************************************/

    public this ( )
    {
        const app_name = "dhtdump";
        const app_desc = "iterates over all channels in a dht node, dumping the"
            " data to disk";
        super(app_name, app_desc, Version);

        this.epoll = new EpollSelectDispatcher;
        this.dht = new DhtClient(this.epoll);

        this.file = new ChannelDumper(new ubyte[IOBufferSize]);

        this.root = new FilePath;
        this.path = new FilePath;
        this.swap_path = new FilePath;
    }


    /***************************************************************************

        Do the actual application work. Called by the super class.

        Params:
            args = command line arguments
            config = parser instance with the parsed configuration

        Returns:
            status code to return to the OS

    ***************************************************************************/

    protected override int run ( Arguments args, ConfigParser config )
    {
        ConfigReader.fill("Dht", this.dht_config, config);
        ConfigReader.fill("Dump", this.dump_config, config);

        this.root.set(this.dump_config.data_dir);

        this.initDht();

        this.initialWait();

        while ( true )
        {
            StopWatch time;
            time.start;

            bool error;
            auto channels = this.getChannels(error);

            log.info("Dumping {} channels", channels.length);

            foreach ( channel; channels )
            {
                log.info("Dumping '{}'", channel);

                ulong records, bytes;
                this.dumpChannel(channel, records, bytes, error);
            }

            this.wait(time.microsec, error);
        }

        return true;
    }


    /***************************************************************************

        Sets up the dht client for use, adding the config-specified node to the
        registry.

    ***************************************************************************/

    private void initDht ( )
    {
        this.dht.addNode(this.dht_config.address, this.dht_config.port);
    }


    /***************************************************************************

        Connects to the dht node and queries the list of channels it contains.

        Params:
            error = set to true if an error occurs while getting the list of
                channels

        Returns:
            list of dht channels

    ***************************************************************************/

    private char[][] getChannels ( ref bool error )
    {
        log.info("Getting list of channels");
        scope ( exit ) log.info("Got list of channels: {}", this.channels);

        void get_dg ( DhtClient.RequestContext, char[] addr, ushort port,
            char[] channel )
        {
            if ( channel.length )
            {
                log.trace("GetChannels: {}:{}, '{}'", addr, port, channel);
                if ( !this.channels.contains(channel) )
                {
                    this.channels.appendCopy(channel);
                }
            }
        }

        void notifier ( DhtClient.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                log.error("DhtClient error during GetChannels: {}",
                    info.message(this.dht.msg_buf));
                error = true;
            }
        }

        this.channels.length = 0;
        this.dht.assign(this.dht.getChannels(&get_dg, &notifier));
        this.epoll.eventLoop();

        return this.channels;
    }


    /***************************************************************************

        Dumps the specified channel to disk.

        Params:
            channel = name of the channel to dump
            records = out value which returns the number of records dumped
            bytes = out value which returns the number of bytes dumped
            error = set to true if an error occurs while dumping

    ***************************************************************************/

    private void dumpChannel ( char[] channel, out ulong records,
        out ulong bytes, ref bool error )
    {
        void get_dg ( DhtClient.RequestContext, char[] key, char[] value )
        {
            if ( key.length && value.length )
            {
                records++;
                bytes += key.length + value.length;
                this.file.write(key, value);
            }
        }

        void notifier ( DhtClient.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                log.error("DhtClient error during GetAll: {}",
                    info.message(this.dht.msg_buf));
                error = true;
            }
        }

        buildFilePath(this.root, this.path, channel).cat(NewFileSuffix);

        if ( this.path.exists() )
        {
            log.warn("{}: OVERWRITING an old, unfinished dump file! "
                "Seems like the dumper crashed.", this.path);
        }

        StopWatch time;
        time.start;

        // Dump channel to file
        {
            this.file.open(this.path.toString);
            scope ( exit ) this.file.close();

            this.dump_path.copy(this.file.path);

            this.dht.assign(this.dht.getAll(channel, &get_dg, &notifier));
            this.epoll.eventLoop();
        }

        this.finalizeChannel(this.dump_path, channel, records, bytes, error,
            time.microsec);
    }


    /***************************************************************************

        Rotates current and backup dump files and cleans up intermediary dump
        files.

        Params:
            filepath = file which channel was dumped to
            channel = name of the channel which was dumped
            records = number of records dumped
            bytes = number of bytes dumped
            error = true if an error occurred while dumping
            dump_microsec = time taken to dump the channel, in microseconds

    ***************************************************************************/

    private void finalizeChannel ( char[] filepath, char[] channel,
        ulong records, ulong bytes, bool error, ulong dump_microsec )
    {
        void removeDumpFile ( )
        {
            this.path.set(filepath);
            this.path.remove();
        }

        if ( error )
        {
            // Delete partial 'channel.dumping' file
            log.warn("Removing partial dump file '{}'", this.path);
            removeDumpFile();
        }
        else if ( records == 0 )
        {
            // If there are no records in the channel, then delete the
            // 'channel.dumping' file
            assert(bytes == 0, "channel dump bytes/records mismatch");
            log.info("Removing empty dump file '{}'", this.path);
            removeDumpFile();
        }
        else
        {
            // Move 'channel' -> 'channel.backup' and 'channel.dumping' ->
            // 'channel' as atomically as possible
            swapNewAndBackupDumps(filepath, channel, this.root, this.path,
                this.swap_path);
        }

        log.info("Finished dumping '{}', {} records, {} bytes, {}s{}", channel,
            records, bytes, dump_microsec / 1_000_000f,
            error ? " [error]" : "");
    }


    /***************************************************************************

        Before the first dump, waits a randomly determined amount of time. This
        is to ensure that, in the situation when multiple instances of this tool
        are started simultaneously, they will not all start dumping at the same
        time, in order to minimise impact on the dht.

    ***************************************************************************/

    private void initialWait ( )
    {
        // Set initial wait time randomly (up to dump_period), to ensure that
        // all nodes are not dumping simultaneously.
        scope rand = new Random;
        uint random_wait;
        rand(random_wait);
        auto wait = random_wait % this.dump_config.period_s;

        log.info("Performing initial dump in {}s (randomized)", wait);
        Thread.sleep(wait);
    }


    /***************************************************************************

        After dumping, waits for the remaining time specified in the config. If
        the remaining time is less than the configured minimum wait time, then
        that period is waited instead.

        Params:
            microsec_active = the time (in microseconds) that the dump procedure
                took. This is subtracted from the configured period to calculate
                the wait time
            error = indicates whether a dht error occurred during the last dump
                cycle

    ***************************************************************************/

    private void wait ( ulong microsec_active, bool error )
    {
        double wait;
        if ( error )
        {
            wait = this.dump_config.retry_wait_s;
            log.warn("Dump not completed successfully. Retrying in {}s", wait);
        }
        else
        {
            wait = this.dump_config.period_s - (microsec_active / 1_000_000f);
            if ( wait < this.dump_config.min_wait_s )
            {
                log.warn("Calculated wait time too short -- either the "
                    "channel dump took an unusually long time, or the "
                    "dump period is set too low in config.ini.");
                wait = this.dump_config.min_wait_s;
            }
            log.info("Finished dumping channels, sleeping for {}s", wait);
        }
        Thread.sleep(wait);
    }
}

