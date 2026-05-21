use tonic::{Request, Response, Status};

use crate::trident::{
    events_server::Events, Event, GetEventRequest, ListEventsRequest, ListEventsResponse,
    StreamEventsRequest,
};

pub struct EventsServiceImpl {
    // TODO: db: sqlx::PgPool
    // TODO: redis: redis::aio::MultiplexedConnection (for StreamEvents)
}

impl EventsServiceImpl {
    pub fn new() -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl Events for EventsServiceImpl {
    /// Return a paginated list of historical events matching the filter.
    async fn list_events(
        &self,
        request: Request<ListEventsRequest>,
    ) -> Result<Response<ListEventsResponse>, Status> {
        let _req = request.into_inner();
        // TODO: build WHERE clause from filter fields
        // TODO: execute paginated query against soroban_events
        // TODO: serialise rows to proto Event messages
        // TODO: compute next_cursor from the last row's id
        Err(Status::unimplemented("list_events not yet implemented"))
    }

    /// Return a single event by UUID.
    async fn get_event(
        &self,
        request: Request<GetEventRequest>,
    ) -> Result<Response<Event>, Status> {
        let _req = request.into_inner();
        // TODO: SELECT * FROM soroban_events WHERE id = $1
        // TODO: return Status::not_found if no row
        Err(Status::unimplemented("get_event not yet implemented"))
    }

    type StreamEventsStream = tokio_stream::wrappers::ReceiverStream<Result<Event, Status>>;

    /// Stream real-time events for a contract from Redis Streams.
    async fn stream_events(
        &self,
        request: Request<StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let _req = request.into_inner();
        // TODO: create a tokio mpsc channel
        // TODO: spawn a task that reads from the Redis stream via XREAD BLOCK
        //       and sends matching events down the channel
        // TODO: return ReceiverStream wrapping the receiver
        Err(Status::unimplemented("stream_events not yet implemented"))
    }
}
