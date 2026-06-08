const duckdb = require('duckdb');
const db = new duckdb.Database(':memory:');

function parseCSV()
{
    //Parse CSV data and call SQL function database updating
    //I should probably make the DuckDB code first
    //Make sure table isn't visible at first

    //But when that uploadButton is pressed:
    //Drop any tables if they exist
    //Call the parseCSV function
    //Which parses the CSV data then inputs it into the table
}
