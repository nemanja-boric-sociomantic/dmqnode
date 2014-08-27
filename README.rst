.. contents::
  :depth: 2

General Notes
^^^^^^^^^^^^^

The notes in this section apply to all of the applications in swarmnodes.

Description
===========

Applications implementing the server-side of the protocols defined in swarm.
Currently contains "nodes" (servers) of three types:

1. Dht node: A memory-based database with data distribution determined by record
   hashes.
2. Logfiles node: A disk-based database with data distribution determined by
   record timestamps.
3. Queue node: A memory-based queue/buffer with no in-built data distribution
   model. (In practice, clients select nodes by round-robin.)

A helper-application for the dht node also exists, called dhtdump. This contacts
a single dht node and periodically backs up its data to disk.

Deployment
==========

Upstart
-------

The swarmnodes applcations are configured to use upstart and will start
automatically upon server reboot. The upstart scripts are located in
``etc/init``. The names of the upstart scripts for the different applications
are given in the following sections.

Manually
--------

To manually start the swarmnodes applications on a server, run
``sudo service <name> start``, where ``<name>`` is the name of the application's
upstart script (see individual sections below). This will start the screen
session and the application. If the application/screen session are already
running, you'll need to shut them down first (also the screen session) before
restarting them.

Design
======

The structure of the nodes' code is based very closely around the structure of
the ``core.node`` package of swarm.

The basic components are:

Select Listener
  The ``swarm.core.node.model.Node : NodeBase`` class, which forms the
  foundation of all swarm nodes, owns an instance of
  ``ocean.net.server.SelectListener : SelectListener``. This provides the basic
  functionality of a server; that is, a listening socket which will accept
  incoming client connections. Each client connection is assigned to a
  connection handler instance from a pool.

Connection Handler Pool
  The select listener manages a pool of connection handlers (derived from
  ``swarm.core.node.connection.ConnectionHandler : ConnectionHandlerTemplate``.
  Each is associated with an incoming socket connection from a client. The
  connection handler reads a request code from the socket and then passes the
  request on to a request handler instance, which is constructed at scope (i.e.
  only exists for the lifetime of the request).

Request Handlers
  A handler class exists for each type of request which the node can handle.
  These are derived from ``swarm.core.node.request.model.IRequest : IRequest``.
  The request handler performs all communication with the client which is
  required by the protocol for the given request. This usually involves
  interacting with the node's storage channels.

Storage Channels
  The ``swarm.core.node.storage.model.IStorageChannels : IStorageChannelsTemplate``
  class provides the base for a set of storage channels, where each channel is
  conceived as storing a different type of data in the system. The individual
  storage channels are derived from
  ``swarm.core.node.storage.model.IStorageEngine : IStorageEngine``.

Dht Node
^^^^^^^^

Description
===========

The dht node is a server which handles requests from the dht client defined in
swarm (``swarm.dht.DhtClient``). One or more nodes make up a complete dht,
though only the client has this knowledge -- individual nodes know nothing of
each others' existence.

Data in the dht node is stored in memory, in instances of the `Tokyo Cabinet`__
memory database, with a separate instance per data channel.

__ http://fallabs.com/tokyocabinet/

Deployment
==========

Screen
------

The dht node runs as root in a screen session called "dht".

Processes
---------

Many machines run multiple instances of the dht node. Check the
`server layout pages`_ for how many there should be for a particular server.
There should be a directory in ``/srv/dht`` for each instance as well, like
``/srv/dht/mem-dht-XX``. Each directory should contain a ``dhtnode``
binary and a ``versions`` folder, containing older binaries (dated).

.. _server layout pages: https://github.com/sociomantic/backend/wiki/Servers#wiki-server-layout

Upstart
-------

The dht node upstart script is located at ``/etc/init/dht.conf``.

Monitoring
==========

Graphing
--------

Graphitus dashboard TBD.

Resource Usage
--------------

A dht node process typically uses 40 to 50% CPU usage, and a very high
proportion of the server's RAM (divided between the number of running instances
-- it is expected that all together the dht nodes instances on a single server
should consume up to 90% of ther server's RAM). Anything beyond this might
indicate a problem.

Checking Everything's OK
------------------------

Console Output
..............

The dht node displays some basic statistics on the console: the range of hashes
it is responsible for, its memory usage [1]_, the number of open connections and
handled records, the number of records and bytes stored, and the time remaining
until the next dump of the memory contents to disk.

.. [1] Note that the memory usage displayed on the console lists the GC managed
   memory. The actual data stored in the node is stored in C-allocated
   (tokyocabinet) memory, so is not listed on the console. Check with ``top``
   (or similar) to get a more accurate picture.

Log Files
.........

The dht node writes two log files:

``root.log``
  Notification of errors when handling requests.

``stats.log``
  Statistics about the number of records and bytes stored (globally and per
  channel), the number of bytes sent and received over the network, and the
  number of open connections and records handled.

Dump Files
..........

The dht node's ``data`` folder should contain one ``.tcm`` file per channel
stored. These are periodically written from the data in memory. When a dump
happens, the old ``.tcm`` file is renamed to ``.tcm.backup``. The ``.tcm`` file
for each channel should have been updated within the last 6 hours.

A cron job runs on the dht servers which makes a daily backup of the ``.tcm``
files in the ``data`` folder. These backups are zipped and stored in
``backup-data``.

Possible Problems
-----------------

Crash
.....

Many applications in the system rely on being able to read and/or write to the
dht. If a single dht node goes down, an equivalent proportion of requests from
client applications will fail. There is currently no fall-back mechanism, beyond
the possibility for the client applications themselves to cache and retry failed
requests. The system is, at this stage, pretty robust; all client applications
can handle the situation where a dht node is inaccessible and reconnect safely
when it returns.

If a dht node crashes while in the middle of dumping its memory data to disk,
all that will happen is that a partly-written temporary file will be found on
the disk. This truncated file can be ignored and will not be loaded by the node
upon restart.

Dump Failure
............

There have been instances in the past where the periodic channel dumping stopped
working. Currently, some dht nodes are performing this periodic dumping
themselves, while (a few) others have handed the duty over to the dht dump
process (see below). If dumping stops working, the procedure in each of these
cases is slightly different:

Dht node
  You can try shutting down the node and hope that the dump which is made at
  shutdown will succeed. If this doesn't succeed, then you'll need to look
  through the backup channel dumps to see if you can find any more useful data
  (i.e. larger dump files).

Dht dump process
  You should be able to simply restart the dht dump process, which should
  reconnect to the node and perform a dump.

Design
======

See section on overall design of the swarm nodes.

Data Flow
=========

Dht nodes do not access any other data stores.

Dependencies
============

:Dependency: libtokyocabinet
:Dependency: liblzo2

Dht Dump
^^^^^^^^

Description
===========

The dht dump process is responsible for saving the in-memory dht data to disk in
a location where the dht node can load it upon startup. One dht dump process
runs per dht node process, on the same server. Each dht dump process is thus
responsible for saving the data stored in a single dht node. As the processes
are running on the same server, the data can be transferred locally, without
going through the network interface.

The dump process spends most of its time sleeping, waking up periodically to
read its dht node's data (via GetAll requests to all channels) and write it to
disk. The period and the location to which the dumped data should be written are
set in the config file.

Note: this process is a replacement for the dump thread which exists in the
currently deployed versions of the dht node.

Deployment
==========

Screen
------

The dht dump process runs as root in a screen session called "dump_dht".

Processes
---------

Many machines run multiple instances of the dht node and should have a matching
count of dht dump processes. Check the `server layout pages`_ for how many there
should be for a particular server. There should be a directory in ``/srv/dht``
for each instance of the dht node, like ``/srv/dht/mem-dht-XX``. Each directory
should contain a ``dump`` folder, which should contain the ``dhtdump`` binary
and a ``versions`` folder, containing older binaries (dated).

.. _`server layout pages`: https://github.com/sociomantic/backend/wiki/Servers#wiki-server-layout

Upstart
-------

The dht dump upstart script is located at ``/etc/init/dhtdump.conf``.

Manually
--------

To manually start the dht dump processes on a server, run ``sudo service dhtdump
start``. This will start the screen session and the dht dump processes.

Monitoring
==========

Graphing
--------

TODO: Graphitus dashboard.

Resource Usage
--------------

A dht dump process typically uses around 40-50Mb of memory and 0% CPU when
sleeping.

Checking Everything's OK
------------------------

Console Output
..............

The dht dump process does not, by default make any console output. The deployed
instances are, however, configured to mirror their log output (see below) to the
console.

Log Files
.........

The dht dump process writes two log files:

``root.log``
  Notification of the process' activity. The latest logline will either indicate
  which channel is being dumped to disk or, while the process is sleeping, the
  time at which the next dump cycle is scheduled to begin.

``stats.log``
  Statistics about the number of records and bytes written per log update (every
  30s) and the size of each channel (in terms of records and bytes) the last
  time it was dumped.

Dump Files
..........

The configured dump location should contain one ``.tcm`` file per channel stored
in the dht node. When a dump happens, the old ``.tcm`` file is renamed to
``.tcm.backup``. The ``.tcm`` file for each channel should have been updated
within the period configured in the dump process' config file (typically 6
hours).

Additionally, a cron job runs on the dht servers which makes a daily backup of
the ``.tcm`` files in the ``data`` folder. These backups are zipped and stored
in ``backup-data``.

Possible Problems
-----------------

Crash
.....

If a dht dump process crashes, the world does not end. It can simply be
restarted when it is noticed that it's no longer running.

If a dht dump process crashes while in the middle of dumping its memory data to
disk, all that will happen is that a partly-written temporary file will be found
on the disk. This truncated file can be ignored and will not be loaded by the
dht node if it restarts.

Design
======

Dht dump is a very simple program. It has the following components:

Dump Cycle
  ``swarmnodes.dht.memory.dhtdump.DumpCycle``. Manages the process of sleeping
  and dumping.

Dht Client
  Owned by the dump cycle. Used to contact the dht node and read the stored
  data. (As only a single node is being contacted, we have to cheat and not
  perform the node handshake, which would fail. This is, in practice, ok, as
  only GetChannels and GetAll requests are performed, which are sent to all
  nodes in the client's registry, without a hash responsibility lookup.)

Dump Stats
  ``swarmnodes.dht.memory.dhtdump.DumpStats``. Aggregates and logs the stats
  output by the process (see above).

Data Flow
=========

The dht dump process accesses all channels in a single dht node, which should be
running on the same server.

Dependencies
============

:Dependency: libebtree
:Dependency: liblzo2

Logfiles Node
^^^^^^^^^^^^^

Description
===========

The logfiles node is a server which handles requests from the dht client defined
in swarm (``swarm.dht.DhtClient``). One or more nodes make up a complete dht,
though only the client has this knowledge -- individual nodes know nothing of
each others' existence.

Data in the logfiles node is stored on disk in a series of folders and files,
based on the channel and timestamp of each record stored.

Deployment
==========

Screen
------

The logfiles node runs as root in a screen session called "logfiles" or
"log-dht".

Processes
---------

Many machines run multiple instances of the logfiles node. Check the
`server layout pages`_ for how many there should be for a particular server.
There should be a directory in ``/srv/dht`` for each instance as well, like
``/srv/dht/log-dht-XX``. Each directory should contain a ``logfilesnode``
binary and a ``versions`` folder, containing older binaries (dated).

Upstart
-------

The logfiles node upstart script is located at ``/etc/init/logfiles.conf``.

Manually
--------

To manually start the logfiles nodes on a server, run ``sudo service logfiles
start``. This will start the screen session and the logfiles node processes.

Monitoring
==========

Graphing
--------

TODO: Graphitus dashboard.

Resource Usage
--------------

A logfiles node process typically requires very little CPU time or RAM, perhaps
10% and a few hundred Mb, respectively. Anything beyond this might indicate a
problem.

Checking Everything's OK
------------------------

Console Output
..............

The logfiles node displays some basic statistics on the console: the range of
timestamps it is responsible for, its memory usage, the number of open
connections and handled records, and the number of records and bytes stored.

Log Files
.........

The logfiles node writes two log files:

``root.log``
  Notification of errors when handling requests.

``stats.log``
  Statistics about the number of records and bytes stored (globally and per
  channel), the number of bytes sent and received over the network, and the
  number of open connections and records handled.

Possible Problems
-----------------

Crash
.....

Many applications in the system rely on being able to read and/or write to the
logfiles dht. If a single logfiles node goes down, an equivalent proportion of
requests from client applications will fail. There is currently no fall-back
mechanism, beyond the possibility for the client applications themselves to
cache and retry failed requests. The system is, at this stage, pretty robust;
all client applications can handle the situation where a logfiles node is
inaccessible and reconnect safely when it returns.

If a logfiles node crashes, it can simply be restarted.

Data Corruption
...............

There have been instances in the past where data for a channel has become
corrupt. This usually happens when a logfiles node fails to cleanly shut down,
either due to a crash or a server reboot. In this case, data which was in the
process of being written may have actually only been partly written to disk,
resulting in invalid data in one or more block files.

This problem is usually not critical, and the logfiles node will continue to
function normally. The data returned by various iteration commands will simply
be truncated.

There is a script in `logfiles_check.py <https://github.com/sociomantic/swarmnodes/blob/master/script/logfiles_check.py>`_
which can parse the logfiles data format, check for errors, and (optionally) fix
them by truncating any subsequent data in the file after the point where an
error is found.

Design
======

See section on overall design of the swarm nodes.

Data Flow
=========

Logfiles nodes do not access any other data stores.

Dependencies
============

:Dependency: liblzo2

Queue Node
^^^^^^^^^^

Description
===========

The queue node is a server which handles requests from the queue client defined
in swarm (``swarm.queue.QueueClient``). One or more nodes make up a complete
queue, though only the client has this knowledge -- individual nodes know
nothing of each others' existence.

Data in the queue node is stored in memory, in fixed-sized, pre-allocated
buffers, one per data channel.

Deployment
==========

Separate Queues
---------------

Currently, there are several completely separate queues which handle different
types of data:

* Tracking queue handling loglines generated by sonar.
* User-matching queue handling usermap records generated by sonar.
* Bidding queue handling bid records generated by thruster.
* Admedia queue handling admedia generated by shore.

To make matters more complicated, this separation is not consistent. The user-
matching records are, for instance, written to the same queue as the tracking
records.

There is no real reason for this separation by data type. The queue itself has
no knowledge about the data it's storing so, given a queue system with
sufficient memory, all data channels could be stored in a single queue system.

Single Nodes
------------

Due to current technical limitations (specifically relating to the handling of
PushMulti and ProduceMulti requests), each of the aforementioned queues consists
of just a single node.

Screen
------

The queue node runs as root in a screen session. The name of the session varies
on different servers to reflect the type of data stored in the node: on most
servers the screen session is named "queue" but on others it may be named
"admedia_queue".

Processes
---------

There should be a directory in ``/srv/queue`` for each instance of the queue
node, like ``/srv/queue/tracking``. Each directory should contain a
``queuenode`` binary and a ``versions`` folder, containing older binaries
(dated).

Upstart
-------

The queue node upstart script is located at ``/etc/init/queue.conf``.

Manually
--------

To manually start the queue nodes on a server, you need to create a new screen
session and run the executables in the appropriate directories.

Monitoring
==========

Graphing
--------

TODO: Graphitus dashboard.

Resource Usage
--------------

A queue node process typically uses up to about 60% CPU usage (depending on
traffic), and a large chunk of RAM -- the config file defines the amount of
memory which is allocated for each channel stored, so the memory usage should be
in the region of <num channels * channel size>.

Checking Everything's OK
------------------------

Console Output
..............

The queue node displays some basic statistics on the console: its memory usage,
the number of open connections and handled records, the number of records and
bytes stored, and the fullness (as a percentage) of each channel.

Log Files
.........

The logfiles node writes two log files:

``root.log``
  Notification of errors when handling requests.

``stats.log``
  Statistics about the number of records and bytes stored (globally and per
  channel), the number of bytes sent and received over the network, and the
  number of open connections and records handled.

Possible Problems
-----------------

Crash
.....

Many applications in the system rely on being able to read and/or write to the
queue. There is, as previously mentioned, at present generally only a single
queue node running for each type of data, so this queue node going down would be
a very bad thing. Requests sent from client applications will simply be lost.
There is currently no fall-back mechanism, beyond the possibility for the client
applications themselves to cache and retry failed requests.

If a queue node crashes, it can simply be restarted.

Design
======

See section on overall design of the swarm nodes.

Data Flow
=========

Queue nodes do not access any other data stores.

Dependencies
============

:Dependency: liblzo2

.. _`server layout pages`: https://github.com/sociomantic/backend/wiki/Servers#wiki-server-layout
