const { CosmosClient } = require('@azure/cosmos');
const endpoint = process.env.COSMOS_DB_ENDPOINT;
const key = process.env.COSMOS_DB_KEY;
const client = new CosmosClient({ endpoint, key });

module.exports = async function (context, myTimer) {
    const databaseId = 'RTP';
    const containerId = 'pageanalytics'; // Raw events container
    const aggContainerId = 'pageanalytics_aggregated'; // Aggregated data container

    const database = client.database(databaseId);
    const rawContainer = database.container(containerId);
    const aggContainer = database.container(aggContainerId);

    const now = new Date();
    const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);

    try {
        // Query for raw events from the last hour
        const querySpec = {
            query: 'SELECT c.domain, COUNT(1) as eventCount FROM c WHERE c._ts >= @timestamp GROUP BY c.domain',
            parameters: [{ name: '@timestamp', value: Math.floor(oneHourAgo.getTime() / 1000) }]
        };

        const { resources: events } = await rawContainer.items.query(querySpec).fetchAll();

        // Insert aggregated data into the aggregate container
        for (const event of events) {
            const aggregatedItem = {
                domain: event.domain,
                eventCount: event.eventCount,
                hour: oneHourAgo.toISOString(),
                id: `${event.domain}_${oneHourAgo.toISOString()}`
            };
            await aggContainer.items.upsert(aggregatedItem);
        }

        context.log('Aggregation completed successfully.');
    } catch (error) {
        context.log('Error during aggregation:', error);
    }
};
