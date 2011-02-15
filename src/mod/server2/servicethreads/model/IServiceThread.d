module mod.server2.servicethreads.model.IServiceThread;

private import mod.server2.util.Terminator;

private import tango.core.Thread;

private import ocean.io.select.model.ISelectListenerInfo;

private import swarm.dht2.node.model.IDhtNode;

private import swarm.dht2.storage.channels.model.IStorageChannelsService;

private import swarm.dht2.storage.model.IStorageEngineService;

debug private import tango.util.log.Trace;



abstract class IServiceThread : Thread
{
    private IStorageChannelsService channels_service;

    private ISelectListenerInfo listener_info;

    private uint update_time;

    public this ( IDhtNode dht, uint update_time )
    {
        this.update_time = update_time;

        this.listener_info = dht.selectListenerInfo();

        this.channels_service = dht.channelsService();

        super(&this.run);
    }

    private void run ( )
    {
        while ( !Terminator.terminating )
        {
            this.serviceNode(this.listener_info, this.update_time);

            foreach ( channel; this.channels_service )
            {
                this.serviceChannel(channel, this.update_time);
            }

            Thread.sleep(this.update_time);
        }
    }

    abstract protected void serviceNode ( ISelectListenerInfo listener_info, uint seconds_elapsed );

    abstract protected void serviceChannel ( IStorageEngineService channel, uint seconds_elapsed );
}

