/*******************************************************************************

    Constant definitions.

    copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved

*******************************************************************************/

module dmqnode.storage.engine.overflow.Const;

struct Const
{
    /***************************************************************************

        File names and suffices.

    ***************************************************************************/

    // Must be char[] because of DMD bug 12634.

    static const char[] datafile_suffix  = ".dat",
                        datafile_name    = "overflow" ~ datafile_suffix,
                        indexfile_suffix = ".csv",
                        indexfile_name   = "ofchannels" ~ indexfile_suffix;

    /***************************************************************************

        A magic string at the beginning of the data file. It may be used as a
        data file version tag.

    ***************************************************************************/

    static const char[8] datafile_id = "QDSKOF01";
}
