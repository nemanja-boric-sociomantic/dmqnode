/*******************************************************************************

    Initializes Global Configuration

    copyright:      Copyright (c) 2009 sociomantic labs. All rights reserved

    version:        Feb 2009: Initial release

    authors:        Thomas Nicolai & Lars Kirchhoff


*******************************************************************************/

module core.config.MainConfig;

/*******************************************************************************

    Imports

********************************************************************************/

public  import ocean.util.Config;

private import ocean.util.OceanException, ocean.util.TraceLog;

private import tango.util.log.Log, tango.util.log.AppendFile;


/*******************************************************************************

    CONFIG PATH

********************************************************************************/


private static char[] config_path = "etc/config.ini";


/*******************************************************************************

    STATIC INITIALIZATION

********************************************************************************/

/**
 * Reads Configuration and append Logfile to OceanException
 *
 * Method gets invoked on the first call. It reads configuration from file and
 * adds logfile output for exceptions thrown. In case the program crashes the
 * exception is written into the error logfile.
 */

import tango.io.device.File;
import tango.io.Console;

static this()
{
    try
        Config.init(config_path);
    catch (Exception e)
        OceanException(e.msg);
    
    TraceLog.init(Config.getChar("Log", "trace"));

    if ( !Config.getInt("Log", "trace_enable") )
        TraceLog.disableTrace;

    Appender error = new AppendFile(Config.getChar("Log", "error"));
    OceanException.setOutput(error);
}
