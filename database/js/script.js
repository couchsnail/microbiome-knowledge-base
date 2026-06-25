/*
    This page handles all of the behind-the-scenes work for the HTML, including
    database handling and updating visual displays. 

    TO-DO:
    **Database Side**
    - Utilize DuckDB and some form of SQL Database in tandem for local hosting
    - Implement fulltext search for better time/memory efficiency
    - Improve query efficency
    
    **Visual/HTML Side**
    - Alter/bugfix checkboxes such that non-vital columns can be hidden initially and 
      voluntarily be displayed
    - Add visual indicator to show why certain table rows were pulled up for custom search
    - Add loader to indicate to user when their local table display is being updated
    - Improve table visibility/readability; currently rather squished (medium priority)
    - Implement pagination (medium priority)
    - Accessibility Checks (medium priority)
    - Improve user experience (low priority)
    - Vertical table scrollbar (low priority)
    - Improve loading efficiency of visual elements (low priority)
    - Make graphics more visually appealing (low priority)

    **Miscellaneous**
    - Error handling for database/HTML sides
    - General code clean-up and documentation
*/

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

/*
    This function enables the functionality that happens after the user presses 
    the "Submit" button for file uploading. It is required to upload a CSV file
    in order for all of the other elements to function. 

    The CSV file is uploaded and through DuckDB, converted into a database table.
    The database rows' data are then dynamically added to the table HTML,
    which renders it onto the page. 

    In the future, this database should be stored locally or via Cloud. 
    Research is currently being conducted on how to accomplish this. 
*/
//Documentation: https://developer.mozilla.org/en-US/docs/Web/API/File
form.addEventListener("submit", async e => {
    e.preventDefault();
    // save the file from the input file
    const file = document.getElementById("myfile").files[0];

    //Documentation: https://duckdb.org/docs/current/clients/wasm/data_ingestion
    if(file) //if file exists
    {
        //This part turns the csv into the DuckDB table
        await db.registerFileHandle('microdata.csv', file, duckdb.DuckDBDataProtocol.BROWSER_FILEREADER, true);

        //Open database connection
        const conn = await db.connect();

        //Documentation: https://duckdb.org/docs/current/data/csv/overview 
        //Erases table if it already exists, then creates it from the CSV file
        await conn.query(`DROP TABLE IF EXISTS micro_data;`)
        await conn.query(`CREATE TABLE IF NOT EXISTS micro_data AS SELECT * FROM 'microdata.csv';`);

        //Current limit is 100 rows
        const result = await conn.query('SELECT * FROM micro_data LIMIT 100;');

        //Displays the table data in HTML
        displayHTML(result);

        //Hide number of relevant search results
        document.getElementById("search_results").style.display = "none";

        //Close database connection
        await conn.close();
    }
    else //error handling for if the CSV file doesn't exist/isn't a CSV file
    {
        alert('Please select a csv file first.');
    }
})

/* Rudimentary search function

    At present, it searches for exact matches to the term entered into the
    search field when the user presses "Search" in every DuckDB row.

    Then it directs the program to display only the relevant results. 

    The present method dynamically builds a query string by searching through 
    every column, casting non-text columns to text. 
    It is extremely slow and not very efficient for a database of this size.

    In the future, I want to implement some form of fulltext search,
    which will dynamically scan table rows much faster by building indices 
    to find partial matches in the form of a search engine. DuckDB has this 
    functionality but I haven't gotten to test it yet.
    Documentation HERE: https://duckdb.org/docs/current/guides/sql_features/full_text_search
    
    A stronger visual update will eventually be added to let the user
    see what flagged the table row as a match and when the table is loaded. 
*/
document.getElementById("custom_attribute_search_form").addEventListener("submit", async(e) =>
{
    e.preventDefault(); 
    
    //Note: Will likely change name of this variable later
    let customAttributes = document.getElementById("customAttribute").value;

    //Test if string is empty
    if(customAttributes == "")
    {
        console.log("Empty");
        alert("Please enter a custom attribute to search for.");
        return;
    }

    //Establish database connection
    const conn = await db.connect();

    //Do not load filtered table if the database is empty/no file loaded
    //There is likely a more efficient way to do this
    const data = await conn.query("SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_name = 'micro_data'");
    let count = data.toArray()[0].count;
    if(count == 0)
    {
        alert("Please upload CSV file first.");
        await conn.close();
        return;
    }


    //Note: Need to error check for if they try to search and the table is empty
    //createSearchString dynamically builds the query string
    let searchConditions = createSearchString(customAttribute.value);
    let query_string = "SELECT * FROM micro_data WHERE " + searchConditions;
    
    //Obtain rows from database table
    let result = await conn.query(query_string);

    //Displays the number of results 
    //This amount is determined from the entire database
    //Not just the limited results
    count = result.toArray().length;
    document.getElementById("numberStudies").innerText = count;
    document.getElementById("search_results").style.display = "block";
    displayHTML(result);

    //Close database connection
    await conn.close(); 
})

//Obtains column data 
function getColumns()
{
    return Array.from(document.querySelectorAll("#header_row th")).map(th => th.textContent.trim());
}

//Dynamically creates query search string based on every column
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

/*
    This function displays database rows' information in an HTML table after taking in
    the query results as a parameter.

    In the future, a visual update will be added to indicate when the page is
    loading the HTML.

    It is called both when the CSV file is uploaded and when the user searches
    for custom attributes/matches across multiple columns. 

    May be updated later for efficiency. 
*/
function displayHTML(result)
{
    //convert query result to an array so it can be used later
    const data = result.toArray();
    let len = data.length;

    //Handles the case where no CSV file has been uploaded
    if(len == 0)
    {
        console.log("CSV empty"); 
        alert("No CSV data found");
        return; 
    }
    
    //Variable that stores the micro_table_body
    let bioBody = document.getElementById("micro_table_body")

    //Clears table if anything was already uploaded
    bioBody.innerHTML = ""; 

    //Documentation for schema: https://github.com/apache/arrow/blob/478286658/js/src/schema.ts#L47
    let col = result.schema.fields.length;
    let columns = result.schema.fields.map(f => f.name);

    //Dynamically builds column headers 
    let thead = document.getElementById("header_row");
    thead.innerHTML = "";
    for(let i = 0; i < col; ++i)
    {
        thead.insertAdjacentHTML("beforeend", "<th>" + columns[i] + "</th>");
    }

    //Dynamically inserts rows into the HTML table
    //By looping through every row and every column in that row
    for(let r = 0; r < len; ++r)
    {
        let row = data[r].toJSON();

        let rowHTML = ""; 

        rowHTML += "<tr>";

        //Length restricted for visibility/readability purposes
        const maxPreviewLength = 20; 

        //Documentation for details: https://developer.mozilla.org/en-US/docs/Web/HTML/Reference/Elements/details
        //Documentation for summary: https://developer.mozilla.org/en-US/docs/Web/HTML/Reference/Elements/summary
        for(let c = 0; c < col; ++c)
        {
            //obtain information for a particular column in a row
            let string = String(row[columns[c]]);

            //Display null if column is null
            if(string == "" || string == null)
            {
                string = "null";
            }

            //Displays a summarized preview that can be expanded to display the full column data
            //For the sake of readability
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

        //Inserts the HTML into the table
        bioBody.insertAdjacentHTML("beforeend", rowHTML);
    }
    toggleAllColumns();
}

/*
    This function affects the visibility of non-vital columns (e.g. country or lat_lon).
    When the checkbox is checked, that column is hidden for readability purposes.

    At present, the checkbox does not reset when custom search is utilized. This error 
    will be fixed in the future.

    Additionally, the results will be flipped in the future; all of the checkbox options
    will be hidden, and when checked, will be displayed. The reason why this is not the
    current implementation is because of assorted visual and display errors that will
    be fixed in future updates. 

    Currently the names of the checkboxes are fixed since the assumption is that 
    the csvs uploaded all follow the same format.

*/
function elementToggle(checkbox_id, checkbox_name)
{
    //Generic function for each elementToggle
    //Documentation for checkbox: https://developer.mozilla.org/en-US/docs/Web/HTML/Reference/Elements/input/checkbox
    document.getElementById(checkbox_id).addEventListener("change", function()
    {
        //Obtain column data
        const columns = getColumns();

        //Obtain the name of a column
        const index = columns.indexOf(checkbox_name); 

        //If column not found, alert the user and cancel function
        if(index == -1)
        {
            alert("Column not found.");
            return;
        }

        //Hides the given column's header
        const header = document.querySelectorAll("#header_row th")[index];
        if(this.checked)
        {
            header.style.display = "none";
        }
        else
        {
            header.style.display = "";
        }
        
        //Loops through every row to hide the cells of the given column
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

//Called here so that the checkbox functionality is always active
toggleAllColumns();

//Adds the ability to hide/show columns to every checkbox
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