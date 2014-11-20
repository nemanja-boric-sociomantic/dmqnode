/*******************************************************************************

    Utility functions to configure tango loggers from a config file.

    copyright:      Copyright (c) 2010 sociomantic labs. All rights reserved

    version:

    authors:        Mathias Baumann

    Configures tango loggers, uses the AppendSyslog class to provide logfile
    rotation.

    In the config file, a logger can be configured using the following syntax:

        ; Which logger to configure. In this case LoggerName is being configured.
        ; A whole hierachy can be specified like LOG.MyApp.ThatOutput.X
        ; And each level can be configured.
        [LOG.LoggerName]

        ; Whether to output to the terminal
        console   = true

        ; File to output to, no output to file if not given
        file      = log/logger_name.log

        ; Whether to propagate the options down in the hierachy
        propagate = false

        ; The verbosity level, corresponse to the tango logger levels
        level     = info

        ; Is this logger additive? That is, should we walk ancestors
        ; looking for more appenders?
        additive  = true

    See the class Config for further options and documentation.

    There are global logger configuration options as well:

        ; Global options are in the section [LOG]
        [LOG]

        ; Maximum amount of files that will exist.
        file_count    = 10

        ; Maximum size of one file in bytes till it will be rotated
        ;
        max_file_size = 500000

        ; files equal or higher this value will be compressed
        start_compress = 4

        ; Buffer size for output
        buffer_size = 2048

    See the class MetaConfig for further options and documentation.

    Upon calling the configureLoggers function, logger related configuration
    will be read and the according loggers configured accordingly.

    Usage Example (you probably will only need to do this):

    ----
        import Log = ocean.util.log.Config;
        // ...
        Log.configureLoggers(Config().iterateCategory!(Log.Config)("LOG"),
                             Config().get!(Log.MetaConfig)("LOG"));
    ----

*******************************************************************************/

module ocean.util.log.Config;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.io.Stdout;
import ocean.core.Array : removePrefix, removeSuffix;
import ocean.util.Config;
import ocean.util.config.ClassFiller;
import ocean.util.config.ConfigParser;
import ocean.text.util.StringSearch;

import tango.util.log.Log;
import tango.util.log.AppendSyslog;
import ocean.util.log.InsertConsole;
import tango.util.log.AppendConsole;

// Log layouts
import ocean.util.log.layout.LayoutMessageOnly;
import ocean.util.log.layout.LayoutStatsLog;
import ocean.util.log.layout.LayoutSimple;
import tango.util.log.LayoutDate;
import tango.util.log.LayoutChainsaw;


/*******************************************************************************

    Configuration class for loggers

*******************************************************************************/

class Config
{
    /***************************************************************************

        Level of the logger

    ***************************************************************************/

    public char[] level;

    /***************************************************************************

        Whether to use console output or not

    ***************************************************************************/

    public SetInfo!(bool) console;

    /***************************************************************************

        Layout to use for console output

    ***************************************************************************/

    public char[] console_layout;

    /***************************************************************************

        Whether to use file output and if, which file path

    ***************************************************************************/

    public SetInfo!(char[]) file;

    /***************************************************************************

        Layout to use for file output

    ***************************************************************************/

    public char[] file_layout;

    /***************************************************************************

        Whether to propagate that level to the children

    ***************************************************************************/

    public bool propagate;

    /***************************************************************************

        Whether this logger should be additive or not

    ***************************************************************************/

    bool additive;

    /***************************************************************************

        Buffer size of the buffer output, overwrites the global setting
        given in MetaConfig

    ***************************************************************************/

    public size_t buffer_size = 0;
}

/*******************************************************************************

    Configuration class for logging

*******************************************************************************/

class MetaConfig
{
    /***************************************************************************

        How many files should be created

    ***************************************************************************/

    size_t file_count    = 10;

    /***************************************************************************

        Maximum size of one log file

    ***************************************************************************/

    size_t max_file_size = 500 * 1024 * 1024;

    /***************************************************************************

        Index of the first file that should be compressed

        E.g. 4 means, start compressing with the fourth file

    ***************************************************************************/

    size_t start_compress = 4;

    /***************************************************************************

        Tango buffer size, if 0, internal stack based buffer of 2048 will be
        used.

    ***************************************************************************/

    size_t buffer_size   = 0;
}

/*******************************************************************************

    Convenience alias for iterating over Config classes

*******************************************************************************/

alias ClassIterator!(Config) ConfigIterator;

/*******************************************************************************

    Convenience alias for layouts

*******************************************************************************/

alias Appender.Layout Layout;

/*******************************************************************************

    Gets a new layout instance, based on the given name.

    Params:
        layout_str = name of the desired layout

    Returns:
        an instance of a suitable layout based on the input string, or an
        instance of 'LayoutMessageOnly' if no suitable layout was identified.

*******************************************************************************/

public Layout newLayout ( char[] layout_str )
{
    Layout layout;

    char[] tweaked_str = layout_str.dup;

    StringSearch!() s;

    s.strToLower(tweaked_str);

    tweaked_str = removePrefix(tweaked_str, "layout");

    tweaked_str = removeSuffix(tweaked_str, "layout");

    switch ( tweaked_str )
    {
        case "messageonly":
            layout = new LayoutMessageOnly;
            break;

        case "stats":
        case "statslog":
            layout = new LayoutStatsLog;
            break;

        case "simple":
            layout = new LayoutSimple;
            break;

        case "date":
            layout = new LayoutDate;
            break;

        case "chainsaw":
            layout = new LayoutChainsaw;
            break;

        default:
            throw new Exception("Invalid layout requested : " ~ layout_str);
    }

    return layout;
}

/*******************************************************************************

    Clear any default appenders at startup

*******************************************************************************/

static this ( )
{
    Log.root.clear();
}

/*******************************************************************************

    Sets up logging configuration.

    Template Params:
        Source = the type of the config parser
        FileLayout = layout to use for logging to file, defaults to LayoutDate
        ConsoleLayout = layout to use for logging to console, defaults to
                        LayoutSimple

    Params:
        config   = an instance of an class iterator for Config
        m_config = an instance of the MetaConfig class
        loose = if true, configuration files will be parsed in a more relaxed
                manner
        use_insert_appender = true if the InsertConsole appender should be used
                              (needed when using the AppStatus module)

*******************************************************************************/

public void configureLoggers ( Source = ConfigParser, FileLayout = LayoutDate,
                               ConsoleLayout = LayoutSimple )
                             ( ClassIterator!(Config, Source) config,
                               MetaConfig m_config, bool loose = false,
                               bool use_insert_appender = false )
{
    enable_loose_parsing(loose);

    foreach (name, settings; config)
    {
        bool console_enabled = false;
        Logger log;

        if ( name == "Root" )
        {
            log = Log.root;
            console_enabled = settings.console(true);
        }
        else
        {
            log = Log.lookup(name);
            console_enabled = settings.console();
        }

        size_t buffer_size = m_config.buffer_size;
        if ( settings.buffer_size )
        {
            buffer_size = settings.buffer_size;
        }

        if ( buffer_size > 0 )
        {
            log.buffer(new char[](buffer_size));
        }

        log.clear();
        // if console/file is specifically set, don't inherit other appenders
        // (unless we have been specifically asked to be additive)
        log.additive = settings.additive ||
                       !(settings.console.set || settings.file.set);

        if ( settings.file.set )
        {
            Layout file_log_layout = (settings.file_layout.length)
                                         ? newLayout(settings.file_layout)
                                         : new FileLayout;

            log.add(new AppendSyslog(settings.file(),
                                     m_config.file_count,
                                     m_config.max_file_size,
                                     "gzip {}", "gz", m_config.start_compress,
                                     file_log_layout));
        }

        if ( console_enabled )
        {
            Layout console_log_layout = (settings.console_layout.length)
                                            ? newLayout(settings.console_layout)
                                            : new ConsoleLayout;

            if ( use_insert_appender )
            {
                log.add(new InsertConsole(console_log_layout));
            }
            else
            {
                log.add(new AppendConsole(console_log_layout));
            }
        }

        with (settings) if ( level.length > 0 )
        {
            StringSearch!() s;

            level = s.strToLower(level);

            switch ( level )
            {
                case "trace":
                case "debug":
                    log.level(Level.Trace, propagate);
                    break;

                case "info":
                    log.level(Level.Info, propagate);
                    break;

                case "warn":
                    log.level(Level.Warn, propagate);
                    break;

                case "error":
                    log.level(Level.Error, propagate);
                    break;

                case "fatal":
                    log.level(Level.Info, propagate);
                    break;

                case "none":
                case "off":
                case "disabled":
                    log.level(Level.None, propagate);
                    break;

                default:
                    throw new Exception("Invalid value for log level in section"
                                        " [" ~ name ~ "]");
            }
        }
    }
}

