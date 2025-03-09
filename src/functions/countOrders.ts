import { app, HttpRequest, HttpResponseInit, InvocationContext } from "@azure/functions";
import { MongoClient } from "mongodb";

export async function countOrders(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
    context.log('HTTP trigger function processed a request.');

    const cosmosDbUri = process.env.COSMOS_DB_CONNECTION_STRING;
    const cosmosDbName = process.env.COSMOS_DB_NAME;
    const cosmosDbCollection = process.env.COSMOS_DB_COLLECTION;

    if (!cosmosDbUri || !cosmosDbName || !cosmosDbCollection) {
        return {
            status: 500,
            body: "Cosmos DB configuration is missing."
        };
    }

    let client: MongoClient;

    try {
        client = new MongoClient(cosmosDbUri);
        await client.connect();

        const database = client.db(cosmosDbName);
        const collection = database.collection(cosmosDbCollection);


        const count = await collection.countDocuments();

        return {
            status: 200,
            body: `Orders Count: ${count}`
        };
    } catch (error) {
        context.error("An error occurred:", error);
        return {
            status: 500,
            body: "An error occurred while saving the transaction."
        };
    } finally {
        await client?.close();
    }
};

app.http('countOrders', {
    methods: ['GET'],
    authLevel: 'function',
    handler: countOrders
});
