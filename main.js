const { Client } = require('@elastic/elasticsearch');
const fs = require('fs');

console.log("Libs are loaded");

async function moveData(sourceEsHost, destEsHost, indexName, query, cursorFile) {
    const sourceClient = new Client({ node: sourceEsHost });
    const destClient = new Client({ node: destEsHost });
  
    const countResponse = await sourceClient.count({
        index: indexName,
        body: {
            query: query.query
        }
    });
    const totalCount = countResponse.body.count;
    console.log(`Amount to process: ${totalCount}`);

    let scrollId;
    let hits = [];
    let processedCount = 0;
    let uploadedCount = 0;

    if (fs.existsSync(cursorFile)) {
        scrollId = fs.readFileSync(cursorFile, 'utf-8').trim().split('\n').pop();
        console.log(`Scroll id exists: ${scrollId}`);
        hits = (await sourceClient.scroll({
            scrollId: scrollId,
            scroll: '24h'
        })).body.hits.hits;
    } else {
        const response = await sourceClient.search({
            index: indexName,
            body: query,
            scroll: '24h',
            size: 500
        });

        scrollId = response.body._scroll_id;
        hits = response.body.hits.hits;
        fs.writeFileSync(cursorFile, `${scrollId}\n`);
        console.log("Scroll id is created and saved to file");
    }


    while (hits.length > 0) {
        const existencePromises = [];

        for (const hit of hits) {
            const existencePromise = destClient.exists({
                index: indexName,
                id: hit._id
            }).then(exists => {
                if (!exists.body) {
                    return destClient.index({
                        index: indexName,
                        id: hit._id,
                        body: hit._source
                    }).then(() => {
                        console.log(`Document with ID ${hit._id} is written`);
                        uploadedCount++;
                    });
                } else {
                    console.log(`Document with ID ${hit._id} exists in destination index.`);
                }
            });

            existencePromises.push(existencePromise);
        }

        await (async () => {while (true) {
            try {
                return await Promise.all(existencePromises);
            } catch (error) {
                console.error(error);
            }
        }})()
        processedCount += existencePromises.length;

        console.log(`Processed ${processedCount}/${totalCount}, Uploaded ${uploadedCount}`);

        const scrollResponse = await sourceClient.scroll({
            scrollId: scrollId,
            scroll: '24h'
        });

        scrollId = scrollResponse.body._scroll_id;
        hits = scrollResponse.body.hits.hits;

        fs.appendFileSync(cursorFile, `${scrollId}\n`);
    }

    console.log(`Data copied from ${sourceEsHost}/${indexName} на ${destEsHost}/${indexName}. Scroll ID saved to ${cursorFile}.`);
}

console.log("Initializing");

(async () => {
    const sourceEsHost = 'http://localhost:19201';
    const destEsHost = 'http://localhost:19202';
    const indexName = 'your_index';
    const query = {
        "query" {"match_all": {}},
        "sort": [
          {
              "_id": {
                  "order": "asc"
              }
          }
      ]
    };
    const cursorFile = 'scroll_ids.txt';

    console.log("Launching");
    await moveData(sourceEsHost, destEsHost, indexName, query, cursorFile);
})();
