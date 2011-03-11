/*******************************************************************************

    copyright:      Copyright (c) 2011 sociomantic labs. All rights reserved

    version:        March 2011: Initial release

    authors:        Gavin Norman

    Interface for storage engine scanner / repairer.

*******************************************************************************/

module src.mod.repair.storagerepair.model.IStorageRepair;



interface IStorageRepair
{
    /***************************************************************************

        Enum describing the modes of the repairer.
    
    ***************************************************************************/

    public enum Mode
    {
        Check,  // check for damage
        Repair  // check for and repair damage
    }


    /***************************************************************************

        Executes a scan / repair over the files for the specified hash range in
        the specified channel.

        Params:
            channel = channel to scan / repair
            start = start of hash range to scan / repair
            end = end of hash range to scan / repair
            mode = process mode (see enum)

    ***************************************************************************/

    public void process ( char[] channel, hash_t start, hash_t end, Mode mode );
}

