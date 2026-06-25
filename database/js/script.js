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

        const result = await conn.query('SELECT * FROM micro_data LIMIT 100;');

        //console.log(result.toArray().map(row => row.toJSON())[1]);

        displayHTML(result);
        document.getElementById("search_results").style.display = "none";

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

//Full-text search which I may need later
//https://duckdb.org/docs/current/guides/sql_features/full_text_search
//Rudimentary custom attributes search
document.getElementById("custom_attribute_search_form").addEventListener("submit", async(e) =>
{
    e.preventDefault(); //not sure if I need this line
    //ideally let it filter stuff without having to do GETs
    //Oh and uhhh customize the "filter form" stuff so I don't have to keep making separate functions idk
    console.log("Custom attribute search");
    let customAttributes = document.getElementById("customAttribute").value;
    console.log("Custom attribute: " + customAttributes); 

    //Test if string is empty
    if(customAttributes == "")
    {
        console.log("Empty");
        alert("Please enter a custom attribute to search for.");
        return;
    }

    const conn = await db.connect();

    const data = await conn.query("SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_name = 'micro_data'");
    let count = data.toArray()[0].count;
    if(count == 0)
    {
        alert("Please upload CSV file first.");
        await conn.close();
        return;
    }


    //Note: Need to error check for if they try to search and the table is empty
    let searchConditions = createSearchString(customAttribute.value);
    let query_string = "SELECT * FROM micro_data WHERE " + searchConditions;
    console.log("Query String: " + query_string);
    let result = await conn.query(query_string);

    console.log(result.toArray().map(row => row.toJSON())[1]);

    count = result.toArray().length;
    document.getElementById("numberStudies").innerText = count;
    document.getElementById("search_results").style.display = "block";
    displayHTML(result);

    await conn.close(); 
})

//Obtain all columns since this is built dynamically
function getColumns()
{
    return Array.from(document.querySelectorAll("#header_row th")).map(th => th.textContent.trim());
}


function createSearchString(searchAttribute)
{
    let searchConditions = "";
    const columns = getColumns();

    let len = columns.length;

    for(let i = 0; i < len; ++i)
    {
        if(i < len - 1)
        {
            searchConditions += "CAST(" + columns[i] + " AS TEXT) LIKE '%" + searchAttribute + "%' OR ";
        }
        else
        {
            searchConditions += "CAST(" + columns[i] + " AS TEXT) LIKE '%" + searchAttribute + "%'";
        }
    }
    return searchConditions;
}


function displayHTML(result)
{
    const data = result.toArray();
    let len = data.length;

    if(len == 0)
    {
        //May need to update this message
        console.log("CSV empty"); 
        alert("No CSV data found");
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
    for(let i = 0; i < col; ++i)
    {
        thead.insertAdjacentHTML("beforeend", "<th>" + columns[i] + "</th>");
    }

    //console.log(columns, Object.keys(data[0].toJSON()));

    for(let r = 0; r < len; ++r)
    {
        let row = data[r].toJSON();
        //if(r === 0) console.log(Object.keys(row));
        //console.log(row);

        let rowHTML = ""; 

        rowHTML += "<tr>";

        const maxPreviewLength = 20; //can make longer if it's too short but due to number of columns

        for(let c = 0; c < col; ++c)
        {
            let string = String(row[columns[c]]);
            if(string == "" || string == null)
            {
                string = "null";
            }

            if(string.length > maxPreviewLength)
            {
                const preview = string.slice(0,maxPreviewLength) + "...";
                rowHTML += "<td><details><summary>" + preview + "</summary>" + string + "</details></td>";
            }
            else
            {
                rowHTML += "<td>" + string + "</td>";
            }
        }

        rowHTML +="</tr>";
        bioBody.insertAdjacentHTML("beforeend", rowHTML);
    }
    toggleAllColumns();
}

//This only works when they're visible initially and not the other way around
function elementToggle(checkbox_id, checkbox_name)
{
    document.getElementById(checkbox_id).addEventListener("change", function()
    {
        const columns = getColumns();
        const index = columns.indexOf(checkbox_name); 
        if(index == -1)
        {
            alert("Column not found.");
            return;
        }
        console.log("Index of column being hidden: " + index);

        //Hides header
        const header = document.querySelectorAll("#header_row th")[index];
        if(this.checked)
        {
            header.style.display = "none";
        }
        else
        {
            header.style.display = "";
        }
        
        const rows = document.querySelectorAll("#bio_table tbody tr");
        const len = rows.length;

        for(let r = 0; r < len; ++r)
        {
            const cell = rows[r].cells[index];
            if(this.checked)
            {
                console.log("Hiding " + checkbox_name);
                cell.style.display = "none";
            }
            else
            {
                console.log("Displaying " + checkbox_name)
                cell.style.display = "";
            }
        }

    })
}

toggleAllColumns();

//onEvent handlers for all checkboxes
//Also need to add error handling and some sort of visual indicator that the table is being reloaded
//Also these do NOT reload properly when like a table is uploaded so...
function toggleAllColumns()
{
    elementToggle("common_name_toggle", "common_name");
    elementToggle("description_toggle", "description");
    elementToggle("bio_material_toggle", "bio_material");
    elementToggle("culture_collection_toggle", "culture_collection");
    elementToggle("specimen_voucher_toggle", "specimen_voucher");
    elementToggle("collected_by_toggle", "collected_by");
    elementToggle("country_toggle", "country");
    elementToggle("identified_by_toggle", "identified_by");
    elementToggle("isolation_source_toggle", "isolation_source");
    elementToggle("lat_lon_toggle", "lat_lon");
    elementToggle("lab_host_toggle", "lab_host");
    elementToggle("environmental_sample_toggle", "environmental_sample");
    elementToggle("mating_type_toggle", "mating_type");
    elementToggle("sex_toggle", "sex");
    elementToggle("cell_type_toggle", "cell_type");
    elementToggle("dev_stage_toggle", "dev_stage");
    elementToggle("tissue_type_toggle", "tissue_type");
    elementToggle("cultivar_toggle", "cultivar");
    elementToggle("ecotype_toggle", "ecotype");
    elementToggle("isolate_toggle", "isolate");
    elementToggle("strain_toggle", "strain");
    elementToggle("sub_species_toggle", "sub_species");
    elementToggle("serotype_toggle", "serotype");
    elementToggle("serovar_toggle", "serovar");
}