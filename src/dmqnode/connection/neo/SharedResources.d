/*******************************************************************************

    Copyright (c) 2016 sociomantic labs. All rights reserved

    DMQ shared resource manager. Handles acquiring / relinquishing of global
    resources by active request handlers.

*******************************************************************************/

module dmqnode.connection.neo.SharedResources;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

/*******************************************************************************

    Resources owned by the node which are needed by the request handlers.

*******************************************************************************/

public final class SharedResources
{
    import ocean.util.container.pool.FreeList;
    import dmqnode.storage.model.StorageChannels;

    /***************************************************************************

        Pool of buffers to store record values in.

    ***************************************************************************/

    private FreeList!(void[]) value_buffers;

    /***************************************************************************

        Reference to the storage channels which the requests are operating on.

    ***************************************************************************/

    public StorageChannels storage_channels;

    /***************************************************************************

        Constructor.

        Params:
            storage_channels = storage channels which the requests are operating
                on

    ***************************************************************************/

    public this ( StorageChannels storage_channels )
    {
        this.storage_channels = storage_channels;

        this.value_buffers = new FreeList!(void[]);
    }

    /***************************************************************************

        Scope class which may be newed inside request handlers to get access to
        the shared pools of resources. Any acquired resources are relinquished
        in the destructor.

    ***************************************************************************/

    public scope class RequestResources
    {
        /***********************************************************************

            Acquired value buffer. null if not acquired.

        ***********************************************************************/

        private void[] acquired_value_buffer;

        /***********************************************************************

            Destructor. Relinquishes any acquired resources back to the shared
            resource pools.

        ***********************************************************************/

        ~this ( )
        {
            if ( this.acquired_value_buffer )
                this.outer.value_buffers.recycle(this.acquired_value_buffer);
        }

        /***********************************************************************

            Gets a record value buffer to be used by the request.

            Returns:
                a new value buffer or the already acquired one, if this method
                has been called before in the lifetime of this object

        ***********************************************************************/

        public void[]* getValueBuffer ( )
        {
            // Acquire new buffer, if not already done
            if ( this.acquired_value_buffer is null )
                this.acquired_value_buffer =
                    this.outer.value_buffers.get(new void[16]);

            // (Re-)initialise the buffer for use
            this.acquired_value_buffer.length = 0;
            enableStomping(this.acquired_value_buffer);

            return &this.acquired_value_buffer;
        }
    }
}
