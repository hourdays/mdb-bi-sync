const { MongoClient } = require('mongodb');
const stream = require('stream');

async function main() {
    /**
     * Connection URI. Update <username>, <password>, and <your-cluster-url> to reflect your cluster.
     * See https://docs.mongodb.com/drivers/node/ for more details
     */
    const source_uri = "mongodb+srv://<user>:<password>@<atlas-cluster-url>/"; // Atlas
    const dest_uri = "mongodb://<user>:<password>@<ip>:<port>/?directConnection=true"; // Legacy
    
    /**
     * The Mongo Client you will use to interact with your database
     * See https://mongodb.github.io/node-mongodb-native/3.6/api/MongoClient.html for more details
     * In case: '[MONGODB DRIVER] Warning: Current Server Discovery and Monitoring engine is deprecated...'
     * pass option { useUnifiedTopology: true } to the MongoClient constructor.
     * const client =  new MongoClient(uri, {useUnifiedTopology: true})
     */
    const source_client = new MongoClient(source_uri); // Atlas
    const dest_client = new MongoClient(dest_uri); // Legacy

    try {
        // Connect to the source MongoDB cluster
        await source_client.connect();
        // Connect to the destination MongoDB cluster
        await dest_client.connect();

        // Make the appropriate DB calls

        /**
         * An aggregation pipeline that matches on new documents not inserted by Legacy change stream
         */
        const pipeline = [
            {
                '$match': {
                    'operationType': 'insert',
                    'fullDocument.sync_source': {$ne : 'legacy-changestream'}
                }
            }
        ];

        // This script contains three ways to monitor new documents in the collection to sync.
        // Comment in the monitoring function you'd like to use.

        // OPTION ONE: Monitor new documents using EventEmitter's on() function.
        await monitorUsingEventEmitter(source_client, 300000, pipeline, dest_client); // 5 minutes?

        // OPTION TWO: Monitor new documents using ChangeStream's hasNext() function
        // await monitorUsingHasNext(client, 30000, pipeline);

        // OPTION THREE: Monitor new documents using the Stream API
        // await monitorUsingStreamAPI(client, 30000, pipeline);

    } finally {
        // Close the connection to the source MongoDB cluster
        await source_client.close();
        // Close the connection to the destination MongoDB cluster
        await dest_client.close();
    }
}

main().catch(console.error);

/**
 * Close the given change stream after the given amount of time
 * @param {*} timeInMs The amount of time in ms to monitor documents
 * @param {*} changeStream The open change stream that should be closed
 */
function closeChangeStream(timeInMs = 60000, changeStream) {
    return new Promise((resolve) => {
        setTimeout(() => {
            console.log("Closing the change stream");
            changeStream.close();
            resolve();
        }, timeInMs)
    })
}

async function getResumeToken(client) {
    const tokensCollection = client.db("<db>").collection("ResumeTokens");
    const tokenDoc = await tokensCollection.findOne({ _id: 'latestToken' });
    return tokenDoc ? tokenDoc.token : null;
}

/**
 * Monitor the collection for changes
 * This function uses the on() function from the EventEmitter class to monitor changes
 * @param {MongoClient} source_client A MongoClient that is connected to the source cluster
 * @param {Number} timeInMs The amount of time in ms to monitor documents
 * @param {Object} pipeline An aggregation pipeline that determines which change events should be output to the console
 * @param {MongoClient} dest_client A MongoClient that is connected to the destination cluster
 */
async function monitorUsingEventEmitter(source_client, timeInMs = 60000, pipeline = [], dest_client) {
    const source_collection = source_client.db("<db>").collection("<collection>");

    // See https://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#watch for the watch() docs
    //const changeStream = source_collection.watch(pipeline);
    const resumeToken = await getResumeToken(source_client);
    const changeStreamOptions = resumeToken ? { resumeAfter: resumeToken } : {};
    const changeStream = source_collection.watch(pipeline, changeStreamOptions);

    // ChangeStream inherits from the Node Built-in Class EventEmitter (https://nodejs.org/dist/latest-v12.x/docs/api/events.html#events_class_eventemitter).
    // We can use EventEmitter's on() to add a listener function that will be called whenever a change occurs in the change stream.
    // See https://nodejs.org/dist/latest-v12.x/docs/api/events.html#events_emitter_on_eventname_listener for the on() docs.
    changeStream.on('change', (next) => {
        console.log(next);

        const fullDocument = next.fullDocument;
        console.log("ChangeStream Full Document:");
        console.log(fullDocument);

        console.log("ChangeStream Full Document + source:");
        fullDocument['sync_source'] = 'atlas-changestream';
        fullDocument['sync_dt'] = new Date();
        console.log(fullDocument);

        // Insert new document into destination cluster so here Legacy
        insertDocument(dest_client, fullDocument);

        // Store the resume token
        const tokensCollection = source_client.db("<db>").collection("ResumeTokens");
        tokensCollection.updateOne(
            { _id: 'latestToken' },
            { $set: { token: next._id } },
            { upsert: true }
        );
    });

    // Wait the given amount of time and then close the change stream
    await closeChangeStream(timeInMs, changeStream);
}

/**
 * Create a new document
 * @param {MongoClient} client A MongoClient that is connected to a cluster
 * @param {Object} document The new document to be added
 */
async function insertDocument(client, document){
    // See https://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#insertOne for the insertOne() docs
    
    console.log("Check if already exists : " + document._id);
    
    // Check for the existence of _id in the collection
    const existingDocument = await client.db("<db>").collection("<collection>").findOne({_id: document._id});

    if (existingDocument) {
        console.log(`Document with _id ${document._id} already exists. Skipping insert.`);
    } else {
        const result = await client.db("<db>").collection("<collection>").insertOne(document);
        console.log(`Document inserted with the following id: ${result.insertedId}`);
    }
}