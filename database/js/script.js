//DuckDB code taken from official documentation: https://duckdb.org/docs/current/clients/wasm/instantiation
import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm/+esm'

const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

// Select a bundle based on browser checks
const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

const worker_url = URL.createObjectURL(
  new Blob([`importScripts("${bundle.mainWorker}");`], {type: 'text/javascript'})
);

// Instantiate the asynchronous version of DuckDB-Wasm
const worker = new Worker(worker_url);
const logger = new duckdb.ConsoleLogger();
const db = new duckdb.AsyncDuckDB(logger, worker); //primary database
await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
URL.revokeObjectURL(worker_url);

const form = document.querySelector("form");

//Note to self: Ask if we want to keep the data persistent/backside later

//Documentation: https://developer.mozilla.org/en-US/docs/Web/API/File
form.addEventListener("submit", async e => {
    e.preventDefault();
    // save the file from the input file
    const file = document.getElementById("myfile").files[0];

    console.log(file);

    //Documentation: https://duckdb.org/docs/current/clients/wasm/data_ingestion
    if(file)
    {
        await db.registerFileHandle('microdata.csv', file, duckdb.DuckDBDataProtocol.BROWSER_FILEREADER, true);

        const conn = await db.connect();

        //Documentation: https://duckdb.org/docs/current/data/csv/overview 
        await conn.query(`DROP TABLE IF EXISTS micro_data;`)
        await conn.query(`CREATE TABLE IF NOT EXISTS micro_data AS SELECT * FROM 'microdata.csv';`);

        const result = await conn.query('SELECT * FROM micro_data LIMIT 10;');
        console.log(result.toArray().map(row => row.toJSON()));

        await conn.close();
    }
    else
    {
        alert('Please select a csv file first.');
    }
    //form.reset();
})

function loadDatabase(data) 
{
    //Parse CSV data and call SQL function database updating
    //I should probably make the DuckDB code first
    //Make sure table isn't visible at first

    //But when that uploadButton is pressed:
    //Drop any tables if they exist
    //Call the parseCSV function
    //Which parses the CSV data then inputs it into the table

    let len = data.length;
    for(let i = 0; i < len; i++)
    {
        
    }
}
