/*******************************************************************************

    Queue test class 

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        September 2011: Initial release

    authors:        Mathias Baumann

*******************************************************************************/

module src.mod.test.SimpleLayout;

private import  tango.text.Util;

private import  tango.time.Clock,
                tango.time.WallClock;

private import  tango.util.log.Log;

private import  Integer = tango.text.convert.Integer;


/*******************************************************************************

        A layout with ISO-8601 date information prefixed to each message
       
*******************************************************************************/

public class SimpleLayout : Appender.Layout
{
        private bool localTime;

        /***********************************************************************
        
                Ctor with indicator for local vs UTC time. Default is 
                local time.
                        
        ***********************************************************************/

        this (bool localTime = true)
        {
                this.localTime = localTime;
        }

        /***********************************************************************
                
                Subclasses should implement this method to perform the
                formatting of the actual message content.

        ***********************************************************************/

        void format (LogEvent event, size_t delegate(void[]) dg)
        {
                char[] level = event.levelName;
                
                // convert time to field values
                auto tm = event.time;
                auto dt = (localTime) ? WallClock.toDate(tm) : Clock.toDate(tm);
                                
                // format date according to ISO-8601 (lightweight formatter)
                char[20] tmp = void;
                char[256] tmp2 = void;
                dg (layout (tmp2, "%0 [%1] - ", 
                            level,
                            event.name
                            ));
                dg (event.toString);
        }

        /**********************************************************************

                Convert an integer to a zero prefixed text representation

        **********************************************************************/

        private char[] convert (char[] tmp, long i)
        {
                return Integer.formatter (tmp, i, 'u', '?', 8);
        }
}