OVERVIEW OF DEPENDENCIES FOR CONNECTION HANDLER, SHARED RESOURCES AND REQUESTS

== src.mod.queue.connection.SharedResources ==

Uses:
swarm.core.common.connection.ISharedResources : SharedResources_T

Defines:
struct QueueConnectionResources
class SharedResources (created by mixin SharedResources_T)


== src.mod.queue.request.model.IQueueRequestResources ==

Uses:
swarm.core.common.request.model.IRequestResources :
    IRequestResources_T,
    RequestResources_T
src.mod.queue.connection.SharedResources : SharedResources

Defines:
interface IRequestResources
          (created by mixin IRequestResources_T(SharedResources))
interface IQueueRequestResources : IRequestResources
scope class RequestResources : IRequestResources
            (created by mixin RequestResources_T(SharedResources))


== src.mod.queue.QueueConnectionHandler ==

Uses:
src.mod.queue.request.model.IQueueRequestResources :
    RequestResources,
    IQueueRequestResources
src.mod.queue.connection.SharedResources : SharedResources

Defines:
QueueConnectionHandler : ConnectionHandlerTemplate
    private scope class QueueRequestResources : RequestResources,
                                                IQueueRequestResources
