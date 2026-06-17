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
        //This part turns the csv into the DuckDB table
        await db.registerFileHandle('microdata.csv', file, duckdb.DuckDBDataProtocol.BROWSER_FILEREADER, true);

        const conn = await db.connect();

        //Documentation: https://duckdb.org/docs/current/data/csv/overview 
        await conn.query(`DROP TABLE IF EXISTS micro_data;`)
        await conn.query(`CREATE TABLE IF NOT EXISTS micro_data AS SELECT * FROM 'microdata.csv';`);

        //For pagination
        /*
        let studiesPerPage = parseInt(getParameterByName('studiesPerPage'), 10)
        if(isNaN(studiesPerPage))
            studiesPerPage = 10; //default
        let pageNumber = parseInt(getParameterByName('page'), 10)
        if(isNaN(pageNumber))
            pageNumber = 1; //default*/

        //const result = loadStudies(studiesPerPage);

        const result = await conn.query('SELECT * FROM micro_data LIMIT 10;');

        //console.log(result.toArray().map(row => row.toJSON())[1]);

        displayHTML(result);

        await conn.close();

        //This is the part where we add it to the table repeatedly
        //Via for-loop
        //There is probably an easier and nicer way to do this
    }
    else
    {
        alert('Please select a csv file first.');
    }
    //form.reset();
})

function loadStudies(studiesPerPage)
{
    let result = conn.query('SELECT * FROM micro_data LIMIT ' + studiesPerPage + ';');
    return result;
}

function displayHTML(result)
{
    const data = result.toArray();
    let len = data.length;

    if(len == 0)
    {
        console.log("CSV empty"); 
        return; 
    }
    
    let bioBody = document.getElementById("micro_table_body")

    //Clears table if anything was already uploaded
    bioBody.innerHTML = ""; 

    //Documentation for schema: https://github.com/apache/arrow/blob/478286658/js/src/schema.ts#L47
    let col = result.schema.fields.length;
    let columns = result.schema.fields.map(f => f.name);

    let thead = document.getElementById("header_row");
    thead.innerHTML = "";
    for(let i = 0; i < col; i++)
    {
        thead.insertAdjacentHTML("beforeend", "<th>" + columns[i] + "</th>");
    }

    //console.log(columns, Object.keys(data[0].toJSON()));

    for(let r = 0; r < len; r++)
    {
        let row = data[r].toJSON();
        //if(r === 0) console.log(Object.keys(row));
        //console.log(row);

        let rowHTML = ""; 

        rowHTML += "<tr>";

        for(let c = 0; c < col; c++)
        {
            rowHTML += "<td>" + row[columns[c]] + "</td>";
        }

        rowHTML +="</tr>";
        bioBody.insertAdjacentHTML("beforeend", rowHTML);
    }
}
