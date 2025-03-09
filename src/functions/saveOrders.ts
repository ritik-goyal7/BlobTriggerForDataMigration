import { app, EventGridEvent, InvocationContext } from "@azure/functions";
import { StorageSharedKeyCredential, BlobServiceClient } from "@azure/storage-blob"
import JSONStream from "JSONStream";
import { Writable } from 'stream';
import { MongoClient, Db } from "mongodb";


function createBlobClient(blobStorageName: string, blobStorageKey: string) {
    const credentials = new StorageSharedKeyCredential(blobStorageName, blobStorageKey);

    return new BlobServiceClient(`https://${blobStorageName}.blob.core.windows.net`, credentials);
}
async function connectDB(connectionString: string, dbName: string, context: InvocationContext): Promise<{ db: Db | null, client: MongoClient | null }> {
    try {
        const client = new MongoClient(connectionString);

        await client.connect();

        context.info("Database connected.");
        return { db: client.db(dbName), client };
    } catch (error) {
        context.error("Failed to connect to Cosmos DB:", error);
        return { db: null, client: null };
    }
}
export async function saveOrders(event: EventGridEvent, context: InvocationContext): Promise<void> {
    const { 
        COSMOS_DB_CONNECTION_STRING,
        COSMOS_DB_NAME,
        COSMOS_DB_COLLECTION,
        BLOB_STORAGE_NAME,
        BLOB_CONTAINER_NAME,
        BLOB_STORAGE_KEY
    } = process.env;

    if (!COSMOS_DB_CONNECTION_STRING || !COSMOS_DB_NAME || !COSMOS_DB_COLLECTION || !BLOB_STORAGE_NAME || !BLOB_CONTAINER_NAME || !BLOB_STORAGE_KEY) {
        context.error("COSMOS_DB_CONNECTION_STRING, COSMOS_DB_NAME, COSMOS_DB_COLLECTION, BLOB_STORAGE_NAME, BLOB_CONTAINER_NAME, BLOB_STORAGE_KEY  are required for saveOrdersToDB function to run.");
        return;
        }
    if (event.eventType !== "Microsoft.Storage.BlobCreated" || !event.subject?.includes(`${BLOB_CONTAINER_NAME}/blobs`)) {
        return;
    }

    const blobName = event.subject.split(`${BLOB_CONTAINER_NAME}/blobs`)[1];
    if (!blobName) {
        context.info(`Exiting as blob not inserted in configured container: ${BLOB_CONTAINER_NAME}`);
    }
    const blobClient = createBlobClient(BLOB_STORAGE_NAME, BLOB_STORAGE_KEY).getContainerClient(BLOB_CONTAINER_NAME).getBlobClient(blobName);

    let client: MongoClient | undefined;
    try {
        const downloadBlockBlobResponse = await blobClient.download();
        const readableStream = downloadBlockBlobResponse.readableStreamBody;

        if (!readableStream) {
            context.error("Failed to get readable stream from blob.");
            return;
        }

        const { db, client: dbClient } = await connectDB(COSMOS_DB_CONNECTION_STRING, COSMOS_DB_NAME, context);
        client = dbClient;
        if (db === null) {
            return;
        }

        const collection = db.collection(COSMOS_DB_COLLECTION);
        const batch: any[] = [];
        const jsonStream = JSONStream.parse('*');

        const writableStream = new Writable({
            objectMode: true,
            write(chunk, encoding, callback) {
                batch.push(chunk);
                if (batch.length >= 100) { // Process in batches of 100
                    // Process the batch here
                    context.log(`Processing batch of ${batch.length} items.`);

                    // insert batch data in db
                    collection.insertMany(batch)
                        .then(() => {
                            context.log(`Inserted batch of ${batch.length} items.`);
                            batch.length = 0; // Clear the batch
                            callback();
                        })
                        .catch(error => {
                            context.error("Error inserting batch:", error);
                            callback(error);
                        });

                    batch.length = 0; // Clear the batch
                }
                callback();
            }
        });

        readableStream.pipe(jsonStream).pipe(writableStream);

        writableStream.on('finish', () => {
            if (batch.length > 0) {
                // Process any remaining items in the batch
                context.log(`Processing final batch of ${batch.length} items.`);
                // insert batch data in db
                collection.insertMany(batch)
                .then(() => {
                    context.log(`Inserted batch of ${batch.length} items.`);
                    batch.length = 0; // Clear the batch
                })
                .catch(error => {
                    context.error("Error inserting batch:", error);
                });
            }
            context.log('Finished processing blob data.');
        });

    } catch (error) {
        context.error(`Error processing blob: ${error}`);
    } finally {
        if (client) {
            await client.close();
            context.log("Database connection closed.");
        }
    }

    context.log('Event grid function processed event:', event);
}

app.eventGrid('saveOrders', {
    handler: saveOrders
});
