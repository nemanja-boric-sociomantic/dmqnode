module mod.server2.servicethreads.StatsThread;

private import mod.server2.servicethreads.model.IServiceThread;

private import ocean.io.select.model.ISelectListenerInfo;

private import ocean.util.TraceLog;

private import swarm.dht2.node.model.IDhtNode;

private import swarm.dht2.storage.model.IStorageEngineService;

debug private import tango.util.log.Trace;



class StatsThread : IServiceThread
{
    public this ( IDhtNode dht, uint update_time )
    {
        super(dht, update_time);
    }

    protected void serviceNode ( ISelectListenerInfo listener_info, uint seconds_elapsed )
    {
        auto received = listener_info.bytesReceived;
        auto sent = listener_info.bytesSent;
        TraceLog.write("Node stats: {} sent ({} K/s), {} received ({} K/s), handling {} connections",
                sent, cast(float)(sent / 1024) / cast(float)seconds_elapsed,
                received, cast(float)(received / 1024) / cast(float)seconds_elapsed,
                listener_info.numOpenConnections);
        listener_info.resetByteCounters();
    }

    protected void serviceChannel ( IStorageEngineService channel, uint seconds_elapsed )
    {
    }
}

