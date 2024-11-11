# elasticsearch_data_migrator
Simple script to migrate your Elasticsearch data from one host to another

I recommend using [elasticdump](https://github.com/elasticsearch-dump/elasticsearch-dump) to migrate the schema and other index settings.

But it is too slow to transfer data and overwrites documents. If you just need to move it to a clean index, then you can speed up if you check whether the document has already been written, increase the number of threads.

It also saves the scroll id for the ability to continue after the break.

Run:
```bash
npm install
node main.js
```
