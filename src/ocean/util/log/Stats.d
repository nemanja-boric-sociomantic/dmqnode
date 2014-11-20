/*******************************************************************************

    copyright:      Copyright (c) 2012 sociomantic labs. All rights reserved

    version:        21/02/2012: Split into StatsLog & PeriodicStats

    authors:        Mathias, Gavin

    Classes to write statistics log files in the standard format expected by
    cacti.

*******************************************************************************/

module ocean.util.log.Stats;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.core.Traits : FieldName;

import ocean.io.select.EpollSelectDispatcher;

import ocean.io.select.client.TimerEvent;

import ocean.text.convert.Layout: StringLayout;

import ocean.util.log.layout.LayoutStatsLog;

import tango.util.log.Log;
import tango.util.log.AppendSyslog;

import tango.stdc.time : time_t;

/*******************************************************************************

    Periodically writes values of an aggregate to a logger using a timer event
    registered with epoll. Uses the statslog format which is:

        date key:value, key:value

    (The date part of the output is handled by the logger layout in
    ocean.util.log.layout.LayoutStatsLog.)

    Template Params:
        T = an aggregate which contains the values that should be written to the
            file, the tuple of the aggregate's members is iterated and each
            printed

    Usage example:

    ---

        import ocean.io.select.EpollSelectDispatcher;
        import ocean.util.log.Stats;

        class MyLogger
        {
            // Epoll instance used for logging timer.
            private const EpollSelectDispatcher epoll;

            // Struct whose fields define the values to write to each line of
            // the stats log file.
            private struct Stats
            {
                bool b = true;
                int x = 23;
                float y = 23.23;
                char[] s = "hello";
            }

            // Instance of Stats struct
            private Stats stats;

            // Periodic logger instance
            private alias PeriodicStatsLog!(Stats) Logger;
            private const Logger logger;

            public this ( EpollSelectDispatcher epoll )
            {
                this.epoll = epoll;
                this.logger = new Logger(epoll, &this.getStats,
                    &this.resetStats);
            }

            // Delegate which is passed to the logger's ctor and is called
            // periodically, and returns a pointer to the struct with the values
            // to be written to the next line in the log file.
            private Stats* getStats ( )
            {
                // Set values of this.stats

                return &this.stats;
            }

            // Delegate which is passed to the logger's ctor and is called after
            // each log line which is written. Used here to reset the stats
            // counters.
            private void resetStats ( StatsLog unused )
            {
                this.stats = this.stats.init;
            }
        }

        auto epoll = new EpollSelectDispatcher;
        auto stats = new MyLogger(epoll);

        // Set everything going (including the stats logging timer).
        epoll.eventLoop();

    ---

*******************************************************************************/

public class PeriodicStatsLog ( T ) : IPeriodicStatsLog
{
    /***************************************************************************

        Delegate to get a pointer to an aggregate containing the values that are
        to be written.

    ***************************************************************************/

    private alias T* delegate ( ) ValueDg;

    private const ValueDg value_dg;

    /***************************************************************************

        Delegate to be called after outputting a line to the log. Provided as a
        convenience to user code, allowing special behaviour (for example the
        resetting of transient stats values) to occur after writing each log.

    ***************************************************************************/

    private alias void delegate ( StatsLog stats_log ) PostLogDg;

    private PostLogDg post_log_dg;

    /***************************************************************************

        Constructor. Registers an update timer with the provided epoll selector.
        The timer first fires 5 seconds after construction, then periodically
        as specified. Each time the timer fires, it calls the user-provided
        delegate, value_dg, which should return a pointer to an aggregate of type
        T containing the values to be written to the next line in the log. Once
        the log line has been written, the optional post_log_dg is called (if
        provided), which may be used to implement special behaviour in the user
        code, such as resetting transient values in the logged aggregate.

        Params:
            epoll    = epoll select dispatcher
            value_dg = delegate to query the current values
            post_log_dg = delegate to call after writing a log line (may be
                null)
            config      = instance of the config class

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, ValueDg value_dg,
                  PostLogDg post_log_dg, Config config )
    {
        with(config) this(epoll, value_dg, post_log_dg,
                          new StatsLog(file_count, max_file_size, file_name),
                          period);
    }


    /***************************************************************************

        Constructor. Registers an update timer with the provided epoll selector.
        The timer first fires 5 seconds after construction, then periodically
        as specified. Each time the timer fires, it calls the user-provided
        delegate, value_dg, which should return a pointer to an aggregate of
        type T containing the values to be written to the next line in the log.
        Once the log line has been written, the optional post_log_dg is called
        (if provided), which may be used to implement special behaviour in the
        user code, such as resetting transient values in the logged aggregate.

        Params:
            epoll    = epoll select dispatcher
            value_dg = delegate to query the current values
            post_log_dg = delegate to call after writing a log line (may be
                null)
            file_count = maximum number of log files before old logs are
                over-written
            max_file_size = size in bytes at which the log files will be rotated
            period   = period after which the values should be written
            file_name = name of log file

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, ValueDg value_dg,
        PostLogDg post_log_dg, size_t file_count = IStatsLog.default_file_count,
        size_t max_file_size = IStatsLog.default_max_file_size,
        time_t period = IStatsLog.default_period,
        char[] file_name = IStatsLog.default_file_name )
    {
        this(epoll, value_dg, post_log_dg,
             new StatsLog(file_count, max_file_size, file_name),
             period);
    }


    /***************************************************************************

        Constructor. Registers an update timer with the provided epoll selector.

        The timer first fires 5 seconds after construction, then periodically
        as specified. Each time the timer fires, it calls the user-provided
        delegate, value_dg, which should return a pointer to an aggregate of
        type T containing the values to be written to the next line in the log.
        Once the log line has been written, the optional post_log_dg is called
        (if provided), which may be used to implement special behaviour in the
        user code, such as resetting transient values in the logged aggregate or
        addition of further values using the StatsLog.add methods. The stats
        values will be written to file after post_log_dg returned.

        Params:
            epoll       = epoll select dispatcher
            value_dg    = delegate to query the current values
            post_log_dg = delegate to call after writing a log line (may be
                          null)
            stats_log   = Stats log instance to use
            period      = period after which the values should be written

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, ValueDg value_dg,
                  PostLogDg post_log_dg, StatsLog stats_log,
                  time_t period = IStatsLog.default_period )
    in
    {
        assert(value_dg !is null, "Value delegate is null");
    }
    body
    {
        this.value_dg = value_dg;
        this.post_log_dg = post_log_dg;

        super(epoll, stats_log, period);
    }


    /***************************************************************************

        Called when the timer event fires. Calls the user's value delegate and
        adds the returned value to the stats log, then calls the optional post-
        log delegate.

    ***************************************************************************/

    protected override void addStats ( )
    {
        this.stats_log.add(*this.value_dg());

        if ( this.post_log_dg )
        {
            this.post_log_dg(this.stats_log);
        }
    }
}


/*******************************************************************************

    Templateless periodic stats log base class.

    The constructors register an update timer with the provided epoll selector.
    The timer first fires 5 seconds after construction, then periodically as
    specified. Each time the timer fires, it calls the abstract method
    addStats() before flushing the logger, writing one line.

*******************************************************************************/

public abstract class IPeriodicStatsLog
{
    /***************************************************************************

        Config class

    ***************************************************************************/

    public static class Config : IStatsLog.Config
    {
        time_t period = IStatsLog.default_period; // 30 seconds
    }

    /***************************************************************************

        Instance of the stats log

    ***************************************************************************/

    protected const StatsLog stats_log;

    /***************************************************************************

        Write period

    ***************************************************************************/

    private const time_t period;

    /***************************************************************************

        Timer which fires to write log output.

    ***************************************************************************/

    private const TimerEvent timer;

    /***************************************************************************

        Construct from config. Registers and starts timer.

        Params:
            epoll = epoll select dispatcher
            config = instance of the config class

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, Config config )
    {
        with(config) this(epoll,
                          new StatsLog(file_count, max_file_size, file_name),
                          period);
    }


    /***************************************************************************

        Construct from individual settings. Registers and starts timer.

        Params:
            epoll = epoll select dispatcher
            file_count = maximum number of log files before old logs are
                over-written
            max_file_size = size in bytes at which the log files will be rotated
            period = period after which the values should be written
            file_name = name of log file

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll,
        size_t file_count = IStatsLog.default_file_count,
        size_t max_file_size = IStatsLog.default_max_file_size,
        time_t period = IStatsLog.default_period,
        char[] file_name = IStatsLog.default_file_name )
    {
        this(epoll,
             new StatsLog(file_count, max_file_size, file_name),
             period);
    }


    /***************************************************************************

        Construct from the provided StatsLog instance. Registers and starts
        timer.

        Params:
            epoll = epoll select dispatcher
            stats_log = Stats log instance to use
            period = period after which the values should be written

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, StatsLog stats_log,
        time_t period = IStatsLog.default_period )
    {
        this.period = period;

        this.timer = new TimerEvent(&this.write_);
        epoll.register(timer);
        timer.set(5, 0, period, 0);

        this.stats_log = stats_log;
    }


    /***************************************************************************

        Called by the timer at the end of each period. Calls the abstract
        addStats() and then flushes the stats to the logger.

    ***************************************************************************/

    private bool write_ ( )
    {
        this.addStats();

        this.stats_log.flush();

        return true;
    }


    /***************************************************************************

        Called when the timer event fires. The method is expected to add any
        desired stats to the log, using the this.stats_log member. The added
        stats will be automatically flushed to the logger.

    ***************************************************************************/

    abstract protected void addStats ( );
}


/*******************************************************************************

   Writes values of an aggregate to a logger. Uses the statslog format which is:

        date key: value, key: value

    (The date part is not written by this class. Instead we rely on the logger
    layout in ocean.util.log.layout.LayoutStatsLog.)

    Usage Example
    ---
    struct MyStats
    {
        size_t bytes_in, bytes_out, awesomeness;
    }

    auto stats_log = new StatsLog(new IStatsLog.Config("log/stats.log",
        10_000, 5));

    MyStats stats;

    stats_log.add(stats)
        .add([ "DynVal" : 34, "DynVal2" : 52])
        .add("dyn3", 62)
        .flush();
    ---

*******************************************************************************/

public class StatsLog : IStatsLog
{
    /***************************************************************************

        Whether to add a separator or not

    ***************************************************************************/

    private bool add_separator = false;

    /***************************************************************************

        Constructor

        Params:
            config = instance of the config class
            name   = name of the logger, should be set to a different string
                     when using more than two StatLogs

    ***************************************************************************/

    public this ( Config config, char[] name = "Stats" )
    {
        super(config, name);
    }


    /***************************************************************************

        Constructor

        Params:
            file_count = maximum number of log files before old logs are
                over-written
            max_file_size = size in bytes at which the log files will be rotated
            file_name = name of the file to write the stats to

    ***************************************************************************/

    public this ( size_t file_count = default_file_count,
        size_t max_file_size = default_max_file_size,
        char[] file_name = default_file_name, char[] name = "Stats" )
    {
        super(new Config(file_name, max_file_size, file_count), name);
    }


    /***************************************************************************

        Constructor

        Uses the same default values for file_count and max_file_size as the
        other constructor.

        Params:
            file_name = name of the file to write the stats to

    ***************************************************************************/

    public this ( char[] file_name, char[] name = "Stats" )
    {
        this(default_file_count, default_max_file_size, file_name, name);
    }


    /***************************************************************************

        Adds one or several values to be outputted to the stats log.

        This function is written using variadic templates to work around the
        limits of template deduction. This function is equivalent to the
        following three functions:

        ************************************************************************
        * Adds the values of the given aggregate to the stats log. Each member
        * of the aggregate will be output as <member name>:<member value>.
        *
        * Params:
        *     values = aggregate containing values to write to the log. Passed
        *         as ref purely to avoid making a copy -- the aggregate is not
        *         modified.
        * ---
        * public typeof(this) add ( T ) ( T values )
        * ---

        ************************************************************************
        * Add another value to the stats
        *
        * Params:
        *     name = name of the value
        *     value = the value to add
        * ---
        * public typeof(this) add ( T ) ( char[] name, T value )
        * ---

        ************************************************************************
        * Add values from an associative array to the stats
        *
        * Params:
        *     values = The associative array with the values to add
        * ---
        * public typeof(this) add ( T ) ( T[char[]] values )
        * ----

        Don't forget to call .flush() after all values have been added.

        Returns:
            A reference to this class so that

            ---
                add(myValues).add("abc", 3).add("efg", 2).flush()
            ---

            can be used to write values

    ***************************************************************************/

    public typeof(this) add ( T... ) ( T parameters )
    {
        static if ( T.length == 1 )
        {   // only parameter an aggregate
            static if ( is ( T[0] == struct ) || is ( T[0] == class ) )
            {
                this.format(parameters[0]);
            }
            else // only parameter not an aggregate, assumed AA
            {
                this.formatAssocArray(parameters[0], this.add_separator);
            }
        }
        // two parameters always (assumed to be) name, value
        else static if ( T.length == 2 )
        {
            this.formatValue(parameters[0], parameters[1], this.add_separator);
        }
        else static assert (false,
                "StatsLog.add(...): called with invalid amount of parameters");

        this.add_separator = true;

        return this;
    }


    /***************************************************************************

        Adds a set of values (denoted by either an aggregate or an associative
        array) to be output to the stats log. The specified string is appended
        to the name of each value written.

        This function is written as a single template to work around the limits
        of template deduction. This function is equivalent to the following two
        functions:

        ************************************************************************
        * Adds the values of the given aggregate to the stats log. Each member
        * of the aggregate will be output as
        * <member name><suffix>:<member value>.
        *
        * Params:
        *     values = aggregate containing values to write to the log. Passed
        *         as ref purely to avoid making a copy -- the aggregate is not
        *         modified.
        *     suffix = suffix to append to the values' names
        * ---
        * public typeof(this) add ( T ) ( T values )
        * ---

        ************************************************************************
        * Add values from an associative array to the stats
        *
        * Params:
        *     values = The associative array with the values to add
        *     suffix = suffix to append to the values' names
        * ---
        * public typeof(this) add ( T ) ( T[char[]] values )
        * ----

        Don't forget to call .flush() after all values have been added.

        Returns:
            A reference to this class for method chaining

    ***************************************************************************/

    public typeof(this) addSuffix ( T ) ( T parameter, char[] suffix )
    {
        // only parameter an aggregate
        static if ( is ( T == struct ) || is ( T == class ) )
        {
            this.formatAggregate(parameter, suffix);
        }
        else // only parameter not an aggregate, assumed AA
        {
            this.formatAssocArray(parameter, this.add_separator, suffix);
        }

        this.add_separator = true;

        return this;
    }


    /***************************************************************************

        Flush everything to file and prepare for the next iteration

    ***************************************************************************/

    public void flush ( )
    {
        this.logger.info(this.layout[]);
        this.add_separator = false;
        this.layout.clear();
    }


    /***************************************************************************

        Formats the values from the provided aggregate to the internal string
        buffer. Each member of the aggregate is formatted as
        <member name>:<member value>.

        Note: When the aggregate is a class, the members of the super class
        are not iterated over.

        Params:
            values = aggregate containing values to format. Passed as ref purely
                to avoid making a copy -- the aggregate is not modified.

        Returns:
            formatted string

    ***************************************************************************/

    private char[] format ( T ) ( ref T values )
    {
        this.formatAggregate(values);

        return this.layout[];
    }


    /***************************************************************************

        Writes the values from the provided aggregate to the format_buffer
        member. Each member of the aggregate is output as
        <member name>:<member value>.

        Note: When the aggregate is a class, the members of the super class
        are not iterated over.

        Params:
            values = aggregate containing values to write to the log. Passed as
                ref purely to avoid making a copy -- the aggregate is not
                modified.
            suffix = optional suffix to append to the values' names

    ***************************************************************************/

    private void formatAggregate ( T ) ( ref T values, char[] suffix = null )
    {
        foreach ( i, value; values.tupleof )
        {
            // stringof results in something like "values.somename", we want
            // only "somename"
            this.formatValue(FieldName!(i, T), value, this.add_separator,
                             suffix);
            this.add_separator = true;
        }
    }
}


/*******************************************************************************

    Templateless stats log base class. Contains no abstract methods, but
    declared as abstract as it is useless on its own, without deriving.

*******************************************************************************/

public abstract class IStatsLog
{
    /***************************************************************************

        Config class

    ***************************************************************************/

    public static class Config
    {
        char[] file_name = default_file_name;
        size_t max_file_size = default_max_file_size;
        size_t file_count = default_file_count;

        this ( char[] file_name, size_t max_file_size, size_t file_count )
        {
            this.file_name = file_name;
            this.max_file_size = max_file_size;
            this.file_count = file_count;
        }

        this(){}
    }

    /***************************************************************************

        Stats log default settings (used in ctor)

    ***************************************************************************/

    public const time_t default_period = 30; // 30 seconds
    public const default_file_count = 10;
    public const default_max_file_size = 10 * 1024 * 1024; // 10Mb
    public const char[] default_file_name = "log/stats.log";


    /***************************************************************************

        Logger instance

    ***************************************************************************/

    protected const Logger logger;


    /***************************************************************************

        Message formatter

    ***************************************************************************/

    protected const StringLayout!() layout;


    /***************************************************************************

        Constructor

        Params:
            file_count = maximum number of log files before old logs are
                over-written
            max_file_size = size in bytes at which the log files will be rotated
            file_name = name of the file to write the stats to

    ***************************************************************************/

    public this ( Config config, char[] name = "Stats" )
    {
        this.logger = Log.lookup(name);
        this.logger.clear();
        this.logger.additive(false);

        this.logger.add(new AppendSyslog(config.file_name, config.file_count,
                                         config.max_file_size, "gzip {}", "gz", 4,
                                         new LayoutStatsLog));

        // Explcitly set the logger to output all levels, to avoid the situation
        // where the root logger is configured to not output level 'info'.
        this.logger.level = this.logger.Level.Trace;

        this.layout = new StringLayout!();
    }


    /***************************************************************************

        Writes the entries of the provided associative array to the
        format_buffer member. Each entry of the associative array is output as
        <key>:<value>.

        Template Params:
            A = type of associative array value. Assumed to be handled by Layout

        Params:
            values = associative array of values to write to the log
            add_separator = flag telling whether a separator (space) should be
                added before a stats value is formatted. After a single value
                has been formatted the value of add_separator is set to true.
            suffix = optional suffix to append to the values' names

    ***************************************************************************/

    protected void formatAssocArray ( A ) ( A[char[]] values, ref bool add_separator,
        char[] suffix = null )
    {
        foreach ( name, value; values )
        {
            this.formatValue(name, value, add_separator, suffix);
            add_separator = true;
        }
    }


    /***************************************************************************

        Writes the specified name:value pair to the format_buffer member.

        Template Params:
            V = type of value. Assumed to be handled by Layout

        Params:
            name = name of stats log entry
            value = value of stats log entry
            add_separator = flag telling whether a separator (space) should be
                added before the stats value is formatted
            suffix = optional suffix to append to the name

    ***************************************************************************/

    protected void formatValue ( V ) ( char[] name, V value, bool add_separator,
        char[] suffix = null )
    {
        if (add_separator)
        {
            this.layout(' ');
        }

        if ( suffix )
        {
            this.layout(name, suffix, ':', value);
        }
        else
        {
            this.layout(name, ':', value);
        }
    }
}
