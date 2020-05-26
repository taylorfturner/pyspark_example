# taylor-turner 

## Clarifications/Assumptions 
- `java --version` returns correctly 
    - If Java returns: ```'java' is not recognized as an internal or external command, operable program or batch file.```
    - Install Java [here](https://www.java.com/en/download/)
- Working in a conda environment. See [Setting Up Conda environment](#setting_up_conda_environment)
- Working with dummy data. See `src/gen_data` for script.
- `mysql-connector-java-5.1.49` is in the root of your `C:\spark\jars\` drive. See [here](https://dev.mysql.com/downloads/connector/j/5.1.html) for download. This is key for the JDBC to pick up the `-bin.jar` file for the MySQL connection.
- Naming convention for 3rd paty vendor software data sources: `DailySales_MMDDYYYY_RESELLER_ID` in `.csv` and `.xml`

## API Clarification 
```
etl = ETL()
df = etl.get() 
df = etl.run(df)
etl.put(df)
```

## Process Flow 
- 1.) Create Database + Table Schema 
- 2.) Populate Tables with Data 
- 3.) Run ETL Script
    - Be restartable if the jobs or subjob fails
    - Support update/insert (UPSERT) execution
    - Handle erroneous data
    - Track data processing metadata (when did the load start, break, finish)
- 4.) Analyze Output

## Setting up Conda Environment 
*Note*: When using VS Code ensure your python interpreter is set to your conda python interpreter 
for both regular code execution and for debugging purposes. 
```
conda create -n toptal python=3.6
conda activate toptal
python setup.py install 
```
