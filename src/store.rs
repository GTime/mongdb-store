use async_trait::async_trait;
use mongodb::{
    bson::{doc, Document},
    Client,
};
use std::marker::PhantomData;
use cqrs_eventsourcing::{
    Aggregate, AggregateContext, DomainEvent, FormatedEvent, FormatedResult, Handlers, MetaData,
    Store, CQRS, Error
};


use crate::mongo;

/// MongodbStore
pub struct MongodbStore<A: Aggregate, E: DomainEvent<A>> {
    client: Client,
    _a: PhantomData<A>,
    _e: PhantomData<E>,
}

impl<A: Aggregate, E: DomainEvent<A>> MongodbStore<A, E> {
    pub fn new(client: Client) -> MongodbStore<A, E> {
        MongodbStore {
            client,
            _a: PhantomData,
            _e: PhantomData,
        }
    }

    /// Creates CQRS with store
    pub fn create_cqrs(client: Client, handlers: Handlers<A, E>) -> CQRS<A, E, MongodbStore<A, E>> {
        CQRS::new(MongodbStore::new(client), handlers)
    }

    /// Retrive Events for query
    pub async fn retrieve_for_query(&self, aggregate_id: Option<&str>) -> FormatedResult<A, E> {
        let mut filter = doc! {"aggregateType": A::aggregate_type()};

        if let Some(x) = aggregate_id {
            filter.insert("aggregate_id", x);
        }

        let formated_events = MongodbStore::find_events(self.client.clone(), Some(filter)).await?;
        Ok(formated_events)
    }

    /// Find Events from store 
    async fn find_events(client: Client, filter: Option<Document>) -> FormatedResult<A, E> {
        let eventstore =
            mongo::Collection::new(client.database("afrodewIdentity").collection("eventstore"));
        let sort = Some(doc! {"version": 1});
        let docs = eventstore.find(filter, sort, None, None).await?;
        let formated_events = FormatedEvent::from_docs(docs).await?;

        Ok(formated_events)
    }
}

impl<A: Aggregate, E: DomainEvent<A>> Clone for MongodbStore<A, E> {
    fn clone(&self) -> MongodbStore<A, E> {
        MongodbStore {
            client: self.client.clone(),
            _a: PhantomData,
            _e: PhantomData,
        }
    }
}

#[async_trait]
impl<A: Aggregate, E: DomainEvent<A>> Store<A, E> for MongodbStore<A, E> {
    /// Rebuilding the aggregate
    async fn assemble_aggregate(&self, id: Option<String>) -> Result<AggregateContext<A>, Error> {
        let mut context = AggregateContext::default();
        context.set_id(id.clone());

        // Populate aggregate if id is provided
        if let Some(x) = id {
            for fmt_event in self.retrieve(x.as_str()).await? {
                fmt_event.payload.clone().apply(&mut context.aggregate);
                context.version = fmt_event.version;
            }
        }

        Ok(context)
    }

    ///  Append formated events to store
    async fn append(
        &self,
        events: Vec<E>,
        context: AggregateContext<A>,
        meta: MetaData,
    ) -> FormatedResult<A, E> {
        let formated_events =
            FormatedEvent::format_events(context.id.as_str(), context.version, events, meta);

        if formated_events.len() == 0 {
            return Ok(Vec::default());
        }

        // Insert into store
        let eventstore = mongo::Collection::new(
            self.client
                .database("afrodewIdentity")
                .collection("eventstore"),
        );

        let docs = FormatedEvent::to_docs(&formated_events)?;
        eventstore.insert_many(docs).await?;

        println!("[MongodbStore: Events Appended]\n");
        Ok(formated_events)
    }

    /// Retrive Events for command store
    async fn retrieve(&self, aggregate_id: &str) -> FormatedResult<A, E> {
        let filter = Some(doc! {"aggregateId": aggregate_id, "aggregateType": A::aggregate_type()});
        let formated_events = MongodbStore::find_events(self.client.clone(), filter).await?;
        Ok(formated_events)
    }
}


