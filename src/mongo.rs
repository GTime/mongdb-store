use futures::stream::StreamExt;
use mongodb::{
    bson::doc,
    bson::{oid::ObjectId, Bson, Document},
    options::{ClientOptions, FindOptions},
    Client,
};

type MongoResult<T> = mongodb::error::Result<T>;
type FindResult = MongoResult<Vec<Document>>;

/// Connect to MongoDB
pub async fn connect(url: Option<&str>, app_name: Option<&str>) -> MongoResult<Client> {
    let url = match url {
        Some(x) => x,
        None => "mongodb://localhost:27017",
    };

    let app_name = match app_name {
        Some(x) => x.to_string(),
        None => "AfroDew Mongo".to_string(),
    };

    // Parse your connection string into an options struct
    let mut client_options = ClientOptions::parse(url).await?;

    // Manually set an option
    client_options.app_name = Some(app_name);

    // Get a handle to the cluster
    let client = Client::with_options(client_options)?;

    // Ping the server to see if you can connect to the cluster
    client
        .database("admin")
        .run_command(doc! {"ping": 1}, None)
        .await?;

    println!("Connected successfully.");
    Ok(client)
}

pub struct Collection {
    collection: mongodb::Collection,
}

impl Collection {
    pub fn new(collection: mongodb::Collection) -> Collection {
        Collection { collection }
    }

    pub async fn insert_many(&self, docs: Vec<Document>) -> FindResult {
        let result = self.collection.insert_many(docs.clone(), None).await?;
        let inserted_ids: Vec<ObjectId> = result
            .inserted_ids
            .values()
            .filter_map(|x| match x.clone() {
                Bson::ObjectId(s) => Some(s),
                _ => None,
            })
            .collect();

        // Find Inserted Docs
        let filter = Some(doc! {"_id": {"$in": inserted_ids } });
        let inserted_docs = self.find(filter, None, None, None).await?;

        Ok(inserted_docs)
    }

    pub async fn find(
        &self,
        filter: Option<Document>,
        sort: Option<Document>,
        skip: Option<i64>,
        limit: Option<i64>,
    ) -> FindResult {
        let find_options = FindOptions::builder()
            .sort(sort)
            .skip(skip)
            .limit(limit)
            .build();
        let mut cursor = self.collection.find(filter, find_options).await?;
        let mut docs: Vec<Document> = Vec::default();

        // Iterate over the results of the cursor.
        while let Some(result) = cursor.next().await {
            match result {
                Ok(document) => {
                    docs.push(document.clone());
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(docs)
    }
}


#[cfg(test)]
mod mongo_test{
    use super::*;

    #[tokio::test]
    async fn test_mongo() -> mongodb::error::Result<()> {
        // Parse your connection string into an options struct
        let client = connect(None, None).await?;
        let collection = client.database("bookReview").collection("books");
        let books_coll = Collection::new(collection);

        let docs = vec![
            doc! { "title": "1984", "author": "George Orwell" },
            doc! { "title": "Animal Farm", "author": "George Orwell" },
            doc! { "title": "The Great Gatsby", "author": "F. Scott Fitzgerald" },
        ];
        let added = books_coll.insert_many(docs).await?;
        // let _books = books_coll.find(None, None, None, None).await?;


        assert_eq!(added.len(), 3);
       

        Ok(())
    }

}