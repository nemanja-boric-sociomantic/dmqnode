/*******************************************************************************

    Logfiles Node Connection Handler

    copyright: Copyright (c) 2014 sociomantic labs. All rights reserved

*******************************************************************************/

module swarmnodes.logfiles.connection.LogfilesConnectionHandler;



/*******************************************************************************

    Imports

*******************************************************************************/

private import swarm.core.node.connection.ConnectionHandler;

private import swarm.core.node.model.INodeInfo;

private import swarm.logfiles.LogfilesConst;

private import swarmnodes.common.kvstore.node.IKVNodeInfo;

private import swarmnodes.common.kvstore.connection.KVConnectionHandler;
private import swarmnodes.common.kvstore.connection.SharedResources;

private import swarmnodes.common.kvstore.request.model.IRequest;
private import swarmnodes.common.kvstore.request.model.IKVRequestResources;

private import swarmnodes.common.kvstore.request.GetVersionRequest;
private import swarmnodes.common.kvstore.request.GetResponsibleRangeRequest;
private import swarmnodes.common.kvstore.request.GetSupportedCommandsRequest;
private import swarmnodes.common.kvstore.request.GetChannelsRequest;
private import swarmnodes.common.kvstore.request.GetSizeRequest;
private import swarmnodes.common.kvstore.request.GetChannelSizeRequest;
private import swarmnodes.common.kvstore.request.GetAllRequest;
private import swarmnodes.common.kvstore.request.GetAllFilterRequest;
private import swarmnodes.common.kvstore.request.RemoveChannelRequest;
private import swarmnodes.common.kvstore.request.GetNumConnectionsRequest;

private import swarmnodes.logfiles.request.PutDupRequest;
private import swarmnodes.logfiles.request.GetRangeRequest;
private import swarmnodes.logfiles.request.GetRangeFilterRequest;

private import swarmnodes.common.kvstore.storage.KVStorageChannels;



/*******************************************************************************

    Logfiles node connection handler class.

    An object pool of these connection handlers is contained in the
    SelectListener which is instantiated inside KVNode.

    TODO: enable HMAC authentication by deriving from HmacAuthConnectionHandler

*******************************************************************************/

public class LogfilesConnectionHandler
    : ConnectionHandlerTemplate!(LogfilesConst.Command)
{
    /***************************************************************************

        Helper class to acquire and relinquish resources required by a request
        while it is handled. The resources are acquired from the shared
        resources instance which is passed to KVConnectionHandler's
        constructor (in the KVConnectionSetupParams instance). Acquired
        resources are automatically relinquished in the destructor.

        Note that it is assumed that each request will own at most one of each
        resource type (it is not possible, for example, to acquire two value
        buffers).

    ***************************************************************************/

    mixin KVRequestResources!();


    /***************************************************************************

        Reuseable exception thrown when the command code read from the client
        is not supported (i.e. does not have a corresponding entry in
        this.requests).

    ***************************************************************************/

    private Exception invalid_command_exception;


    /***************************************************************************

        Constructor.

        Params:
            finalize_dg = user-specified finalizer, called when the connection
                is shut down
            setup = struct containing setup data for this connection

    ***************************************************************************/

    public this ( FinalizeDg finalize_dg, ConnectionSetupParams setup )
    {
        super(finalize_dg, setup);

        this.invalid_command_exception = new Exception("Invalid command",
            __FILE__, __LINE__);
    }


    /***************************************************************************

        Command code 'None' handler. Treated the same as an invalid command
        code.

    ***************************************************************************/

    override protected void handleNone ( )
    {
        this.handleInvalidCommand();
    }


    /***************************************************************************

        Command code 'GetVersion' handler.

    ***************************************************************************/

    override protected void handleGetVersion ( )
    {
        this.handleCommand!(GetVersionRequest);
    }


    /***************************************************************************

        Command code 'GetResponsibleRange' handler.

    ***************************************************************************/

    override protected void handleGetResponsibleRange ( )
    {
        this.handleCommand!(GetResponsibleRangeRequest);
    }


    /***************************************************************************

        Command code 'GetSupportedCommands' handler.

    ***************************************************************************/

    override protected void handleGetSupportedCommands ( )
    {
        this.handleCommand!(GetSupportedCommandsRequest);
    }


    /***************************************************************************

        Command code 'GetNumConnections' handler.

    ***************************************************************************/

    override protected void handleGetNumConnections ( )
    {
        this.handleCommand!(GetNumConnectionsRequest);
    }


    /***************************************************************************

        Command code 'GetChannels' handler.

    ***************************************************************************/

    override protected void handleGetChannels ( )
    {
        this.handleCommand!(GetChannelsRequest);
    }


    /***************************************************************************

        Command code 'GetSize' handler.

    ***************************************************************************/

    override protected void handleGetSize ( )
    {
        this.handleCommand!(GetSizeRequest);
    }


    /***************************************************************************

        Command code 'GetChannelSize' handler.

    ***************************************************************************/

    override protected void handleGetChannelSize ( )
    {
        this.handleCommand!(GetChannelSizeRequest);
    }


    /***************************************************************************

        Command code 'Put' handler.

    ***************************************************************************/

    override protected void handlePut ( )
    {
        // TODO: remove this method when this command is removed from the list
        // of codes
        throw this.invalid_command_exception;
    }


    /***************************************************************************

        Command code 'PutDup' handler.

    ***************************************************************************/

    override protected void handlePutDup ( )
    {
        this.handleCommand!(PutDupRequest);
    }


    /***************************************************************************

        Command code 'Get' handler.

    ***************************************************************************/

    override protected void handleGet ( )
    {
        // TODO: remove this method when this command is removed from the list
        // of codes
        throw this.invalid_command_exception;
    }


    /***************************************************************************

        Command code 'GetAll' handler.

    ***************************************************************************/

    override protected void handleGetAll ( )
    {
        this.handleCommand!(GetAllRequest);
    }


    /***************************************************************************

        Command code 'GetAllFilter' handler.

    ***************************************************************************/

    override protected void handleGetAllFilter ( )
    {
        this.handleCommand!(GetAllFilterRequest);
    }


    /***************************************************************************

        Command code 'GetAllKeys' handler.

    ***************************************************************************/

    override protected void handleGetAllKeys ( )
    {
        // TODO: remove this method when this command is removed from the list
        // of codes
    }


    /***************************************************************************

        Command code 'GetRange' handler.

    ***************************************************************************/

    override protected void handleGetRange ( )
    {
        this.handleCommand!(GetRangeRequest);
    }


    /***************************************************************************

        Command code 'GetRangeFilter' handler.

    ***************************************************************************/

    override protected void handleGetRangeFilter ( )
    {
        this.handleCommand!(GetRangeFilterRequest);
    }


    /***************************************************************************

        Command code 'Listen' handler.

    ***************************************************************************/

    override protected void handleListen ( )
    {
        // TODO: remove this method when this command is removed from the list
        // of codes
        throw this.invalid_command_exception;
    }


    /***************************************************************************

        Command code 'Exists' handler.

    ***************************************************************************/

    override protected void handleExists ( )
    {
        // TODO: remove this method when this command is removed from the list
        // of codes
        throw this.invalid_command_exception;
    }


    /***************************************************************************

        Command code 'Remove' handler.

    ***************************************************************************/

    override protected void handleRemove ( )
    {
        // TODO: remove this method when this command is removed from the list
        // of codes
        throw this.invalid_command_exception;
    }


    /***************************************************************************

        Command code 'RemoveChannel' handler.

    ***************************************************************************/

    override protected void handleRemoveChannel ( )
    {
        this.handleCommand!(RemoveChannelRequest);
    }


    /***************************************************************************

        Command code 'Redistribute' handler.

    ***************************************************************************/

    override protected void handleRedistribute ( )
    {
        // TODO: remove this method when this command is removed from the list
        // of codes
        throw this.invalid_command_exception;
    }


    /***************************************************************************

        Called when an invalid command code is read from the connection.

    ***************************************************************************/

    override protected void handleInvalidCommand_ ( )
    {
        super.writer.write(LogfilesConst.Status.E.InvalidRequest);
    }


    /***************************************************************************

        Command handler method template.

        Template params:
            Handler = type of request handler

    ***************************************************************************/

    private void handleCommand ( Handler : IRequest ) ( )
    {
        scope resources = new KVRequestResources;
        scope handler = new Handler(this.reader, this.writer, resources);

        // calls handler.handle() and checks memory and buffer allocation after
        // request finishes
        this.handleRequest!(KVConnectionResources)(handler, resources);
    }
}

