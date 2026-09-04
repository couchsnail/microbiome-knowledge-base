/*
    This page handles all of the behind-the-scenes work for the HTML, including
    database handling and updating visual displays. 

    TO-DO:
    **Database Side**
    - Implement fulltext search for better time/memory efficiency (medium-low priority)
    - Improve query efficency (low priority)
    - Sort table by given specifications (medium priority)
    - Some sort of read/write/review thing (low priority)
    - Add somewhere where like these updated rows can be added but also edited if need be
    - Fix search on Database Side (high priority)
    
    **Visual/HTML Side**
    - Add visual indicator to show why certain table rows were pulled up for custom search
    - Add loader to indicate to user when their local table display is being updated
    - Improve table visibility/readability; currently rather squished (medium priority)
    - Implement pagination (medium priority)
    - Accessibility Checks (medium priority)
    - Improve user experience (low priority)
    - Vertical table scrollbar (low priority)
    - Improve loading efficiency of visual elements (low priority)
    - Make graphics more visually appealing (low priority)
    - Add more means of sorting (e.g. by reviewer, sequencing type, diseased)

    For first tab: Add option to continually keep pulling up studies and then exporting to CSV
    and/or uploading to database

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

console.log("script loaded");

const study_table_body_states = {'currentPage': 1, 'totalPages': 1, 'rowDisplay': 20, 'searchParameters':""};
const micro_table_body_states =  {'currentPage': 1, 'totalPages': 1, 'rowDisplay': 20, 'searchParameters':""};
const data_table_body_states =  {'currentPage': 1, 'totalPages': 1, 'rowDisplay': 20, 'searchParameters':""};

//Function for hiding/showing tabs
//Taken from here: https://www.w3schools.com/howto/howto_js_tabs.asp
//Done by default at the start
function openTab(event, tabName)
{
    console.log("tab being opened");
    let i = 0;

    let tabContent = document.getElementsByClassName("tabcontent");
    let tabLen = tabContent.length;
    for (i = 0; i < tabLen; ++i) 
    {
        tabContent[i].style.display = "none";
    }

    let tabLinks = document.getElementsByClassName("tablinks");
    let linLen = tabLinks.length;
    for (i = 0; i < linLen; ++i) 
    {
        tabLinks[i].className = tabLinks[i].className.replace(" active", "");
    }

    document.getElementById(tabName).style.display = "block";
    event.currentTarget.className += " active";
}

/* This function loads the database */
async function loadDatabase()
{
    try
    {
        const response = await fetch("http://127.0.0.1:8000/database");
        console.log(response);

        let result = await response.json();
        console.log("Results " + result);

        console.log("Result data: " + result.data);

        let dataStr = JSON.stringify(result.data);
        if(dataStr == "")
        {
            console.log("dataStr empty");
            return;
        }
        sessionStorage.setItem("databaseInfo", dataStr);
        console.log("Session storage set");

        console.log("Database converted to str");

        const conn = await db.connect();
        console.log("Database connected");

        await db.registerFileText("database_info.json", dataStr);
        console.log("Temp file created");

        //Named as such after what the actual PostGreSQL database is named
        //May change name later
        await conn.query('DROP TABLE IF EXISTS complete_database;');
        console.log("Dropped table if it already exists");

        await conn.insertJSONFromPath("database_info.json", {
            name: "complete_database",
            schema: "main",
            create: true
        });
        console.log("Table created/updated");
        
        //Selects all of the columns in the database. Currently inconsistent with other SELECT statements
        //because the columns aren't showing up in the right order. 
        let data_entries = await conn.query("SELECT source_study, accession, alias, center_name, " + 
            "broker_name, title, taxon_id, scientific_name, common_name, description," + 
            "bio_material, culture_collection, specimen_voucher, collected_by," + 
            "collection_date, country, host, identified_by, isolation_source," + 
            "lat_lon, lab_host, environmental_sample, mating_type, sex," + 
            "cell_type, dev_stage, tissue_type, cultivar, ecotype," + 
            "isolate, strain, sub_species, cell_line, serotype, serovar," +
            "custom_attributes," + 
            "tier, review_reason, year_reviewed, journal, n_samples," + 
            "repository_link, paper_link, disease_evidence, disease_present," + 
            "disease_from_names, age_present, sex_present, antibiotic_present," +
            "geography_present, body_site_group, sequencing_type, disease_group," +
            "age_key, sex_key, disease_key, reviewer, reviewer_agrees, reviewer_notes" +
            " FROM complete_database");
            
        console.log("Table results fetched");
        //Close database connection
        await conn.close();
        console.log("Successfully retrieved database"); 

        //Display study data in tables
        console.log("Displaying Database HTML now");
        displayHTML(data_entries, "data_table_body", "data_header_row");
        console.log("Database HTML successfully displayed");
    }
    catch(error)
    {
        console.log(error)
    }
}

//Switches tabs
document.getElementById("defaultOpen").addEventListener("click", function(e) 
{
    openTab(e, "access_data");
});

document.getElementById("csvTab").addEventListener("click", function(e) 
{
    openTab(e, "view_csv");
});

document.getElementById("dataTab").addEventListener("click", async(e) =>
{
    openTab(e, "view_database");

    await loadDatabase();
});

document.getElementById("defaultOpen").click();

//Note to self: Ask if we want to keep the data persistent/backside later

//==============Code for Access Data Tab====================
//Calls getAccession to sift through all accession code stuff
document.getElementById("accession_search_form").addEventListener("submit", async(e) =>
{
    e.preventDefault();
    console.log("Submit fired");
    let accession_code = document.getElementById("accessionCodeSearch").value;

    await getAccession(accession_code);

});

//Adds accession data to database
document.getElementById("database_save").addEventListener("click", async(e) =>
{
    e.preventDefault();
    console.log("Trying to add study data to database");

    addToDatabase(); 
});

//Documentation HERE: https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API/Using_Fetch
//DuckDB Documentation: https://duckdb.org/docs/lts/data/json/loading_json
//DuckDB JSON from Path Documentation: https://shell.duckdb.org/docs/classes/index.DuckDBConnection.html#insertjsonfrompath

/* Obtains study data from a particular accession code.

   It passes in the accession code to the API, which calls fetch in main.py.
   If successful, it will return the study data in the form of a dictionary
   and save it in sessionStorage so it can be added to the database later. 

   If it fails, there will be an error message (recovery for this hasn't been implemented yet)

   The study data will then be loaded into a local DuckDB database.
*/
async function getAccession(accessionCode)
{
    try
    {
        console.log("Response successful");
        const responseString = "http://127.0.0.1:8000/fetch/" + accessionCode;
        const response = await fetch(responseString);
        let result = await response.json();
        console.log("Response result received");

        console.log(result.status);
        console.log(result.accession);
        console.log(result.data);

        //Open database connection
        const conn = await db.connect();
        console.log("Database connected");

        //let query_string = "CREATE TABLE IF NOT EXISTS study_data AS SELECT * FROM read_json_auto(?," + result.data + ");";
        let dataStr = JSON.stringify(result.data);
        console.log("JSON stringified");
        sessionStorage.setItem("studyData", dataStr);
        console.log("Saved in session storage");
        await db.registerFileText("study_data.json", dataStr);
        console.log("Registered as file text");
        //Two separate tables so we don't cross-contaminate the tables in the other tabs
        
        await conn.query('DROP TABLE IF EXISTS study_data;') 
        console.log("Table dropped");
        //await conn.query(`CREATE TABLE study_data AS SELECT * FROM read_json_auto('study_data.json');`);
        await conn.insertJSONFromPath("study_data.json", {
            name: "study_data",
            schema: "main",
            create: true
        });
        console.log("Table created");
        
        /*let study_results = await conn.query("SELECT source_study, accession, alias, center_name, " + 
            "broker_name, title, taxon_id, scientific_name, common_name, description," + 
            "bio_material, culture_collection, specimen_voucher, collected_by," + 
            "collection_date, country, host, identified_by, isolation_source," + 
            "lat_lon, lab_host, environmental_sample, mating_type, sex," + 
            "cell_type, dev_stage, tissue_type, cultivar, ecotype," + 
            "isolate, strain, sub_species, cell_line, serotype, serovar," +
            "custom_attributes FROM study_data;");*/
        let study_results = await conn.query("SELECT * FROM study_data");
        console.log("Table results fetched");

        //Close database connection
        await conn.close();
        console.log("Successfully uploaded database"); 

        //Display study data in tables
        console.log("Displaying HTML now");
        displayHTML(study_results, "study_table_body", "study_header_row");
        console.log("HTML successfully displayed");

        //alert("Study Data accessed successfully");
    }
    catch(error)
    {
        console.log(error);
    }
}

/* Adds the DuckDB database of the accession code queried study data
   and adds it to the database.

   This is accomplished by getting the study information from sessionStorage,
   then calling the API's submit function. Followed by a call to loadDatabase()
   so the database reloads with the new information. (in the middle of debugging)
   
   Note: Converts JSON text into JavaScript value
*/

async function addToDatabase()
{
    let studyInfo = sessionStorage.getItem("studyData"); 

    if(studyInfo == "" || !studyInfo)
    {
        print("Study info empty");
        alert("No study data to upload. Please enter accession code.");
        return; 
    }

    try
    {
        let data = JSON.parse(studyInfo); 
        console.log("Data parsed");

        const response = await fetch("http://127.0.0.1:8000/submit", 
        {
            method: "POST",
            headers: 
            {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({ dataframe: data })
        });

        const result = await response.json();
        
        console.log(JSON.stringify(result, null, 2));
        //alert("Study data uploaded to database.");
    }
    catch(error)
    {
        console.log(error);
    }

    await loadDatabase();
}

/* This function allows the user to download the study data in the DuckDB database
   into a CSV file.

   An option to download as a TSV will be added in the future.
*/

document.getElementById("study_csv_download").addEventListener("click", async(e) =>
{
    e.preventDefault();
    console.log("Downloading as CSV file");

    let studyInfo = sessionStorage.getItem("studyData"); 

    if(studyInfo == "" || !studyInfo)
    {
        alert("No study data to download as CSV. Please enter accession code.");
        return; 
    }

    try
    {
        let data = JSON.parse(studyInfo); 
        console.log("Data parsed");

        const response = await fetch("http://127.0.0.1:8000/download", 
        {
            method: "POST",
            headers: 
            {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({ dataframe: data })
        });

        const result = await response.json();
        
        console.log(JSON.stringify(result, null, 2));
        //alert("Study data uploaded to database.");
    }
    catch(error)
    {
        console.log(error);
    }
});

//==============Code for View CSV Tab=======================

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
document.getElementById("file_form").addEventListener("submit", async e => {
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

        let totalRows = await conn.query("SELECT COUNT(*) AS count FROM micro_data");
        let rows = totalRows.toArray()[0].count;
        console.log("Rows: " + rows);
        micro_table_body_states['totalPages'] = Math.ceil(Number(rows) / 20);
        
        let limit = micro_table_body_states['rowDisplay'];
        let offset = 0;

        const result = await conn.query("SELECT * FROM micro_data LIMIT " + limit + " OFFSET " + offset + ";");

        //Displays the table data in HTML
        displayHTML(result, "micro_table_body", "header_row");

        //Hide number of relevant search results
        document.getElementById("search_results").style.display = "none";

        //Close database connection
        await conn.close();


        document.getElementById("microPageCount").innerText = micro_table_body_states['currentPage'];
        document.getElementById("microPageTotal").innerText = micro_table_body_states['totalPages'];
        document.getElementById("micro_page_counts").style.display = "block";

        //make next and prev buttons enabled
        let prevButton = document.getElementById("micro_prev_button");
        prevButton.style.display = "";
        prevButton.disabled = true;

        let nextButton = document.getElementById("micro_next_button");
        nextButton.style.display = "";
        nextButton.disabled = false;
    }
    else //error handling for if the CSV file doesn't exist/isn't a CSV file
    {
        alert('Please select a csv file first.');
    }
})

document.getElementById("micro_prev_button").addEventListener("click", async e => {
    e.preventDefault();
    console.log("Prev button clicked");
    const conn = await db.connect();

    let page = micro_table_body_states['currentPage'];
    console.log("Current page: " + page);
    let totalPages = micro_table_body_states['totalPages'];
    let limit = micro_table_body_states['rowDisplay'];

    console.log("Calculating new page");
    page = page - 1; 
    console.log("New page count: " + page);

    if(page <= 0)
    {
        console.log("Page out of bounds < 0");
        return; 
    }
    
    if(page == 1)
    {
        console.log("Disabling prev button");
        //perhaps also put this in displayHTML
        let prevButton = document.getElementById("micro_prev_button");
        prevButton.disabled = true;
    }

    if(page <= totalPages)
    {
        console.log("Enabling next button");
        let nextButton = document.getElementById("micro_next_button");
        nextButton.disabled = false;
    }

    console.log("Updating current page display");
    micro_table_body_states['currentPage'] = page;

    document.getElementById("microPageCount").innerText = micro_table_body_states['currentPage'];

    console.log("Calculating offset");
    let offset = (page - 1) * limit; 
    console.log("Offset: " + offset);

    console.log("Displaying next table page");

    let params = micro_table_body_states['searchParameters'];
    let result; 
    if(params)
    {
        result = await conn.query("SELECT * FROM micro_data WHERE " + params + " LIMIT + " + limit + " OFFSET " + offset + ";");
    }
    else
    {
        result = await conn.query("SELECT * FROM micro_data LIMIT " + limit + " OFFSET " + offset + ";");
    
    }
    displayHTML(result, "micro_table_body", "header_row");

    await conn.close(); 
})

document.getElementById("micro_next_button").addEventListener("click", async e => {
    e.preventDefault();
    console.log("Next button clicked");
    const conn = await db.connect();

    let page = micro_table_body_states['currentPage'];
    console.log("Current page: " + page);
    let totalPages = micro_table_body_states['totalPages'];
    let limit = micro_table_body_states['rowDisplay'];

    console.log("Calculating new page");
    page = page + 1; 
    console.log("New page count: " + page);

    if(page > totalPages)
    {
        return;
    }

    if(page >= 1)
    {
        let prevButton = document.getElementById("micro_prev_button");
        prevButton.disabled = false; 
    }
    
    if(page == totalPages)
    {
        //perhaps also put this in displayHTML
        let nextButton = document.getElementById("micro_next_button");
        nextButton.disabled = true;
    }

    let prevButton = document.getElementById("micro_prev_button");
    prevButton.disabled = false;

    micro_table_body_states['currentPage'] = page;
    document.getElementById("microPageCount").innerText = micro_table_body_states['currentPage'];

    console.log("Calculating offset");
    let offset = (page - 1) * limit; 

    let params = micro_table_body_states['searchParameters'];
    let result; 
    if(params)
    {
        result = await conn.query("SELECT * FROM micro_data WHERE " + params + " + LIMIT + " + limit + " OFFSET " + offset + ";");
    }
    else
    {
        result = await conn.query("SELECT * FROM micro_data LIMIT " + limit + " OFFSET " + offset + ";");
    
    }   displayHTML(result, "micro_table_body", "header_row");
    
    await conn.close(); 
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

    9/1/26 NOTE: For some reason this function's counterpart isn't working for the View Database tab. 
*/
document.getElementById("custom_attribute_search_form").addEventListener("submit", async(e) =>
{
    e.preventDefault(); 
    console.log("Searching from CSV");

    //Note: Will likely change name of this variable later
    let customAttributes = document.getElementById("customAttribute").value;
    let header = "header_row";
    console.log("Custom attribute: " + customAttributes);

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
    console.log("Count from information schema: " + count);
    if(count == 0)
    {
        alert("Please upload CSV file first.");
        await conn.close();

        document.getElementById("microPageCount").innerText = 0;
        document.getElementById("microPageTotal").innerText = 0;
        document.getElementById("micro_page_counts").style.display = "none";

        //make next and prev buttons enabled
        let prevButton = document.getElementById("micro_prev_button");
        prevButton.style.display = "none";
        prevButton.disabled = true;

        let nextButton = document.getElementById("micro_next_button");
        nextButton.style.display = "none";
        nextButton.disabled = true;
        return;
    }


    console.log("Creating search string");
    //Note: Need to error check for if they try to search and the table is empty
    //createSearchString dynamically builds the query string

    //We do the search twice here effectively; more efficient way to do this probably, but this is the best we've got right now
    let searchConditions = createSearchString(customAttributes, header);
    micro_table_body_states['searchParameters'] = searchConditions;
    /*let query_string = "SELECT source_study, accession, alias, center_name, " + 
            "broker_name, title, taxon_id, scientific_name, common_name, description," + 
            "bio_material, culture_collection, specimen_voucher, collected_by," + 
            "collection_date, country, host, identified_by, isolation_source," + 
            "lat_lon, lab_host, environmental_sample, mating_type, sex," + 
            "cell_type, dev_stage, tissue_type, cultivar, ecotype," + 
            "isolate, strain, sub_species, cell_line, serotype, serovar," +
            "custom_attributes FROM micro_data WHERE " + searchConditions + 
            " LIMIT " + limit + " OFFSET " + offset;*/
    let query_count = "SELECT COUNT(*) AS count FROM micro_data WHERE " + searchConditions;
     
    console.log("Query string: " + query_count);
    //Obtain rows from database table
    let rowCounts = await conn.query(query_count);

    let rows = rowCounts.toArray()[0].count;
    console.log("Total rows: " + rows);
    console.log("Total pages: " + Math.ceil(Number(rows) / 20));

    micro_table_body_states['currentPage'] = 1; 
    micro_table_body_states['totalPages'] = Math.ceil(Number(rows) / 20);

    let limit = micro_table_body_states['rowDisplay'];
    let offset = 0;

    let query_string = "SELECT * FROM micro_data WHERE " + searchConditions + " LIMIT " + limit + " OFFSET " + offset;
    let result = await conn.query(query_string);

    console.log("Count query: " + query_count);
    console.log("Data query: " + query_string);

    document.getElementById("numberStudies").innerText = rows;
    document.getElementById("search_results").style.display = "block";

    document.getElementById("microPageCount").innerText = micro_table_body_states['currentPage'];
    document.getElementById("microPageTotal").innerText = micro_table_body_states['totalPages'];
    document.getElementById("micro_page_counts").style.display = "block";

    //make next and prev buttons enabled
    let prevButton = document.getElementById("micro_prev_button");
    prevButton.style.display = "";
    prevButton.disabled = true;

    let nextButton = document.getElementById("micro_next_button");
    nextButton.style.display = "";
    nextButton.disabled = false;

    displayHTML(result, "micro_table_body", "header_row");

    //Close database connection
    await conn.close(); 
})


//Obtains column data 
function getColumns(headerName)
{
    return Array.from(document.querySelectorAll("#" + headerName + " th")).map(th => th.textContent.trim());
}

//Dynamically creates query search string based on every column
function createSearchString(searchAttribute, headerName)
{
    let searchCopy = searchAttribute.toLowerCase(); 
    let searchConditions = "";
    const columns = getColumns(headerName);

    let len = columns.length;

    /*for(let i = 0; i < len; ++i)
    {
        if(i < len - 1)
        {
            searchConditions += "LOWER(CAST(" + columns[i] + " AS TEXT)) LIKE '%" + searchCopy + "%' OR ";
        }
        else
        {
            searchConditions += "LOWER(CAST(" + columns[i] + " AS TEXT)) LIKE '%" + searchCopy + "%'";
        }
    }*/

    searchConditions = "LOWER(CAST(custom_attributes AS TEXT)) LIKE '%" + searchCopy + "%'";
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
function displayHTML(result, tableBody, headerName)
{
    //convert query result to an array so it can be used later
    const data = result.toArray();
    let len = data.length;

    //Handles the case where no CSV file has been uploaded
    if(len == 0)
    {
        console.log("CSV empty"); 
        alert("No data found");
        return; 
    }
    
    //Variable that stores the micro_table_body
    let bioBody = document.getElementById(tableBody)

    //Clears table if anything was already uploaded
    bioBody.innerHTML = ""; 

    //Documentation for schema: https://github.com/apache/arrow/blob/478286658/js/src/schema.ts#L47
    let col = result.schema.fields.length;
    let columns = result.schema.fields.map(f => f.name);

    let colgroup = document.getElementById(tableBody + "_colgroup");

    //Dynamically builds column headers
    //colgroup documentation: https://developer.mozilla.org/en-US/docs/Web/HTML/Reference/Elements/colgroup
    //col documentation: https://developer.mozilla.org/en-US/docs/Web/HTML/Reference/Elements/col 
    //createElement documentation: https://developer.mozilla.org/en-US/docs/Web/API/Document/createElement
    let thead = document.getElementById(headerName); //This may cause problems later given that it's called header_row in two separte tables. Will need to see effects
    thead.innerHTML = "";
    
    for(let i = 0; i < col; ++i)
    {
        let coli = document.createElement("col");
        coli.id = "col_" + columns[i] + "_" + tableBody;
        //console.log("coli.id: " + coli.id);
        colgroup.appendChild(coli);
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
    
    //Documentation for the list: https://developer.mozilla.org/en-US/docs/Web/CSS/Guides/Selectors
    document.querySelectorAll(".checkbox_group input[type='checkbox']").forEach(checkbox => {
        checkbox.dispatchEvent(new Event("change"));
    })
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
    the csvs uploaded all follow the same format. More may be added as we reassess and update
    the table schemas. 

*/

//colgroup documentation: https://developer.mozilla.org/en-US/docs/Web/HTML/Reference/Elements/colgroup
function elementToggle(checkbox_id, checkbox_name, tableBody, headerName)
{
    //Generic function for each elementToggle
    //Documentation for checkbox: https://developer.mozilla.org/en-US/docs/Web/HTML/Reference/Elements/input/checkbox
    document.getElementById(checkbox_id).addEventListener("change", function()
    {
        //Obtain column data
        //console.log("Header: " + headerName);
        const columns = getColumns(headerName);

        //Obtain the name of a column
        //console.log("Checkbox name: " + checkbox_name);
        const index = columns.indexOf(checkbox_name); 

        //console.log("Checkbox index: " + index);

        //If column not found, alert the user and cancel function
        /*if(index == -1)
        {
            alert("Column not found.");
            return;
        }*/

        //console.log("Adjusting: " + "col_" + checkbox_name + "_" + tableBody);
        if(this.checked)
        {
            document.getElementById("col_" + checkbox_name + "_" + tableBody).style.visibility = "collapse";
        }
        else
        {
            document.getElementById("col_" + checkbox_name + "_" + tableBody).style.visibility = "";
        }   
    })
}

//Called here so that the checkbox functionality is always active
//toggleAllColumns();

//Adds the ability to hide/show columns to every checkbox. There may be a more efficient way to do this
function toggleAllColumns()
{
    //For the View CSV tab
    elementToggle("common_name_toggle", "common_name", "micro_table_body", "header_row");
    elementToggle("description_toggle", "description", "micro_table_body", "header_row");
    elementToggle("bio_material_toggle", "bio_material", "micro_table_body", "header_row");
    elementToggle("culture_collection_toggle", "culture_collection", "micro_table_body", "header_row");
    elementToggle("specimen_voucher_toggle", "specimen_voucher", "micro_table_body", "header_row");
    elementToggle("collected_by_toggle", "collected_by", "micro_table_body", "header_row");
    elementToggle("country_toggle", "country", "micro_table_body", "header_row");
    elementToggle("identified_by_toggle", "identified_by", "micro_table_body", "header_row");
    elementToggle("isolation_source_toggle", "isolation_source", "micro_table_body", "header_row");
    elementToggle("lat_lon_toggle", "lat_lon", "micro_table_body", "header_row");
    elementToggle("lab_host_toggle", "lab_host", "micro_table_body", "header_row");
    elementToggle("environmental_sample_toggle", "environmental_sample", "micro_table_body", "header_row");
    elementToggle("mating_type_toggle", "mating_type", "micro_table_body", "header_row");
    elementToggle("sex_toggle", "sex", "micro_table_body", "header_row");
    elementToggle("cell_type_toggle", "cell_type", "micro_table_body", "header_row");
    elementToggle("dev_stage_toggle", "dev_stage", "micro_table_body", "header_row");
    elementToggle("tissue_type_toggle", "tissue_type", "micro_table_body", "header_row");
    elementToggle("cultivar_toggle", "cultivar", "micro_table_body", "header_row");
    elementToggle("ecotype_toggle", "ecotype", "micro_table_body", "header_row");
    elementToggle("isolate_toggle", "isolate", "micro_table_body", "header_row");
    elementToggle("strain_toggle", "strain", "micro_table_body", "header_row");
    elementToggle("sub_species_toggle", "sub_species", "micro_table_body", "header_row");
    elementToggle("serotype_toggle", "serotype", "micro_table_body", "header_row");
    elementToggle("serovar_toggle", "serovar", "micro_table_body", "header_row");
    
    //For the View Database tab
    elementToggle("common_name_data_toggle", "common_name", "data_table_body", "data_header_row");
    elementToggle("description_data_toggle", "description", "data_table_body", "data_header_row");
    elementToggle("bio_material_data_toggle", "bio_material", "data_table_body", "data_header_row");
    elementToggle("culture_collection_data_toggle", "culture_collection", "data_table_body", "data_header_row");
    elementToggle("specimen_voucher_data_toggle", "specimen_voucher", "data_table_body", "data_header_row");
    elementToggle("collected_by_data_toggle", "collected_by", "data_table_body", "data_header_row");
    elementToggle("country_data_toggle", "country", "data_table_body", "data_header_row");
    elementToggle("identified_by_data_toggle", "identified_by", "data_table_body", "data_header_row");
    elementToggle("isolation_source_data_toggle", "isolation_source", "data_table_body", "data_header_row");
    elementToggle("lat_lon_data_toggle", "lat_lon", "data_table_body", "data_header_row");
    elementToggle("lab_host_data_toggle", "lab_host", "data_table_body", "data_header_row");
    elementToggle("environmental_sample_data_toggle", "environmental_sample", "data_table_body", "data_header_row");
    elementToggle("mating_type_data_toggle", "mating_type", "data_table_body", "data_header_row");
    elementToggle("sex_data_toggle", "sex", "data_table_body", "data_header_row");
    elementToggle("cell_type_data_toggle", "cell_type", "data_table_body", "data_header_row");
    elementToggle("dev_stage_data_toggle", "dev_stage", "data_table_body", "data_header_row");
    elementToggle("tissue_type_data_toggle", "tissue_type", "data_table_body", "data_header_row");
    elementToggle("cultivar_data_toggle", "cultivar", "data_table_body", "data_header_row");
    elementToggle("ecotype_data_toggle", "ecotype", "data_table_body", "data_header_row");
    elementToggle("isolate_data_toggle", "isolate", "data_table_body", "data_header_row");
    elementToggle("strain_data_toggle", "strain", "data_table_body", "data_header_row");
    elementToggle("sub_species_data_toggle", "sub_species", "data_table_body", "data_header_row");
    elementToggle("serotype_data_toggle", "serotype", "data_table_body", "data_header_row");
    elementToggle("serovar_data_toggle", "serovar", "data_table_body", "data_header_row");
}

/* Code for View Database Tab*/
//Note: Has redundant code because it's not working if I try separating out at present.
//Has the same functionality as the previous function, but is currently not working. 
document.getElementById("custom_attribute_search_form_database").addEventListener("submit", async(e) =>
{
    e.preventDefault(); 
    console.log("Searching database");
    
    //Note: Will likely change name of this variable later
    let customAttributes = document.getElementById("customAttributeData").value;
    let header = "data_header_row";
    console.log("Custom attribute: " + customAttributes);

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
    
    const data = await conn.query("SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_name = 'complete_database'");
    let count = data.toArray()[0].count;
    console.log("Count from information schema: " + count);
    if(count == 0)
    {
        alert("Please add data to database.");
        await conn.close();

        document.getElementById("microPageCount").innerText = micro_table_body_states['currentPage'];
        document.getElementById("microPageTotal").innerText = micro_table_body_states['totalPages'];
        document.getElementById("micro_page_counts").style.display = "block";

        //make next and prev buttons enabled
        let prevButton = document.getElementById("micro_prev_button");
        prevButton.style.display = "";
        prevButton.disabled = true;

        let nextButton = document.getElementById("micro_next_button");
        nextButton.style.display = "";
        nextButton.disabled = false;
        return;
    }

    console.log("Creating search string");
    //Note: Need to error check for if they try to search and the table is empty
    //createSearchString dynamically builds the query string
    let searchConditions = createSearchString(customAttributes, header);
    /*let query_string = "SELECT source_study, accession, alias, center_name, " + 
            "broker_name, title, taxon_id, scientific_name, common_name, description," + 
            "bio_material, culture_collection, specimen_voucher, collected_by," + 
            "collection_date, country, host, identified_by, isolation_source," + 
            "lat_lon, lab_host, environmental_sample, mating_type, sex," + 
            "cell_type, dev_stage, tissue_type, cultivar, ecotype," + 
            "isolate, strain, sub_species, cell_line, serotype, serovar," +
            "custom_attributes," + 
            "tier, review_reason, year_reviewed, journal, n_samples," + 
            "repository_link, paper_link, disease_evidence, disease_present," + 
            "disease_from_names, age_present, sex_present, antibiotic_present," +
            "geography_present, body_site_group, sequencing_type, disease_group," +
            "age_key, sex_key, disease_key, reviewer, reviewer_agrees, reviewer_notes" +
            " FROM complete_database WHERE " + searchConditions;*/
    let query_string = "SELECT * FROM complete_database WHERE " + searchConditions;
    console.log("Query string: " + query_string);
    //Obtain rows from database table
    let result = await conn.query(query_string);

    //Displays the number of results 
    //This amount is determined from the entire database
    //Not just the limited results
    count = result.toArray().length;
    console.log("Result length: " + count);

    document.getElementById("numberStudiesData").innerText = count;
    document.getElementById("search_results_data").style.display = "block";
    displayHTML(result, "data_table_body", "data_header_row");

    //Close database connection
    await conn.close(); 
})

/* Adds the option to download the database as a CSV. In the future, will add 
   option to download as TSV.
*/
document.getElementById("database_csv_download").addEventListener("click", async(e) =>
{
    e.preventDefault();
    console.log("Downloading database as CSV file");

    let studyInfo = sessionStorage.getItem("databaseInfo"); 

    if(studyInfo == "" || !studyInfo)
    {
        alert("No database table to download as CSV. Please add to the database.");
        return; 
    }

    try
    {
        let data = JSON.parse(studyInfo); 
        console.log("Data parsed");

        const response = await fetch("http://127.0.0.1:8000/download", 
        {
            method: "POST",
            headers: 
            {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({ dataframe: data })
        });

        const result = await response.json();
        
        console.log(JSON.stringify(result, null, 2));
        //alert("Study data uploaded to database.");
    }
    catch(error)
    {
        console.log(error);
    }
});
