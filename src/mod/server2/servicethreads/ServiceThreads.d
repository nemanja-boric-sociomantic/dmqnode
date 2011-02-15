module mod.server2.servicethreads.ServiceThreads;



private import mod.server2.servicethreads.model.IServiceThread;



class ServiceThreads
{
    private IServiceThread[] threads;

    public void add ( IServiceThread thread )
    {
        this.threads ~= thread;
    }

    public void start ( )
    {
        foreach ( thread; this.threads )
        {
            thread.start();
        }
    }
}

