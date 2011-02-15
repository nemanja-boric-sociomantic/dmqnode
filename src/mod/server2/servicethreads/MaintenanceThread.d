module mod.server2.servicethreads.MaintenanceThread;

private import mod.server2.servicethreads.model.IServiceThread;

private import ocean.io.select.model.ISelectListenerInfo;

private import swarm.dht2.node.model.IDhtNode;

private import swarm.dht2.storage.model.IStorageEngineService;

debug private import tango.util.log.Trace;


class MaintenanceThread : IServiceThread
{
    public this ( IDhtNode dht, uint update_time )
    {
        super(dht, update_time);
    }

    protected void serviceNode ( ISelectListenerInfo listener_info, uint seconds_elapsed )
    {
    }

    protected void serviceChannel ( IStorageEngineService channel, uint seconds_elapsed )
    {
        channel.maintenance();
    }
}

