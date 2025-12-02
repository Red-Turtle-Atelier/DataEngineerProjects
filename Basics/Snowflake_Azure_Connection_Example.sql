/*--
In this Worksheet we will walk through templated SQL for the end to end process required
to load data from Amazon S3, Microsoft Azure and Google Cloud into a table.

    Helpful Snowflake Documentation:
        1. Bulk Loading from Amazon S3 - https://docs.snowflake.com/en/user-guide/data-load-s3
        2. Bulk Loading from Microsoft Azure - https://docs.snowflake.com/en/user-guide/data-load-azure
        3. Bulk Loading from Google Cloud Storage - https://docs.snowflake.com/en/user-guide/data-load-gcs
--*/


-------------------------------------------------------------------------------------------
    -- Step 1: To start, let's set the Role and Warehouse context
        -- USE ROLE: https://docs.snowflake.com/en/sql-reference/sql/use-role
        -- USE WAREHOUSE: https://docs.snowflake.com/en/sql-reference/sql/use-warehouse
-------------------------------------------------------------------------------------------

--> To run a single query, place your cursor in the query editor and select the Run button (⌘-Return).
--> To run the entire worksheet, select 'Run All' from the dropdown next to the Run button (⌘-Shift-Return).

---> set Role Context
USE ROLE ACCOUNTADMIN;

---> set Warehouse Context
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

---> set the Database
USE DATABASE SNOWFLAKE_LEARNING_DB;

---> set the Schema
SET user_name = current_user();
SET schema_name = CONCAT($user_name, '_LOAD_DATA_FROM_CLOUD');
USE SCHEMA IDENTIFIER($schema_name);


-------------------------------------------------------------------------------------------
    -- Step 2: Create Table
        -- CREATE TABLE: https://docs.snowflake.com/en/sql-reference/sql/create-table
-------------------------------------------------------------------------------------------

---> create the Table
CREATE OR REPLACE TABLE sales_bronze
    (
    customer_name varchar(255)
    ,item_name varchar(255)
    ,quantity int
    ,purchase_date datetime
    --> supported types: https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
    )
    COMMENT = 'Dataset for testing Azure and Databricks Pipelines';

---> query the empty Table
SELECT * FROM sales_bronze;


-------------------------------------------------------------------------------------------
    -- Step 3: Create Storage Integrations
        -- CREATE STORAGE INTEGRATION: https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration
-------------------------------------------------------------------------------------------

    /*--
      A Storage Integration is a Snowflake object that stores a generated identity and access management
      (IAM) entity for your external cloud storage, along with an optional set of allowed or blocked storage locations
      (Amazon S3, Google Cloud Storage, or Microsoft Azure).
    --*/

---> Create the Microsoft Azure Storage Integration
    -- Configuring an Azure Container for Loading Data: https://docs.snowflake.com/en/user-guide/data-load-azure-config

CREATE STORAGE INTEGRATION my_azure_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  AZURE_TENANT_ID = '6ca753bc-a6dc-4e2b-b398-4f82f9fe14d7'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('azure://redturtleatelier.blob.core.windows.net/test-data/');    --('azure://<bucket>/<path>/');

    /*--
      Execute the command below to retrieve the AZURE_CONSENT_URL and AZURE_MULTI_TENANT_APP_NAME for the client application created
      automatically for your Snowflake account. You’ll use these values to configure permissions for Snowflake in your Azure Management Console:
          https://docs.snowflake.com/en/user-guide/data-load-azure-config#step-2-grant-snowflake-access-to-the-storage-locations
    --*/

---> Describe our Integration
    -- DESCRIBE INTEGRATIONS: https://docs.snowflake.com/en/sql-reference/sql/desc-integration

DESCRIBE INTEGRATION my_azure_integration;



SHOW STORAGE INTEGRATIONS;


-------------------------------------------------------------------------------------------
    -- Step 6: Create Stage Objects
-------------------------------------------------------------------------------------------

    /*--
      A stage specifies where data files are stored (i.e. "staged") so that the data in the files
      can be loaded into a table.
    --*/



---> Create the Microsoft Azure Stage
    -- Creating an Azure Stage: https://docs.snowflake.com/en/user-guide/data-load-azure-create-stage

CREATE  STAGE IF NOT EXISTS my_azure_stage
URL = 'azure://redturtleatelier.blob.core.windows.net/test-data/'
STORAGE_INTEGRATION = my_azure_integration -- created in previous step

;


---> View our Stages
    -- SHOW STAGES: https://docs.snowflake.com/en/sql-reference/sql/show-stages

SHOW STAGES;


-------------------------------------------------------------------------------------------
    -- Step 7: Load Data from Stages
-------------------------------------------------------------------------------------------


---> Load data from the Azure Stage into the Table
    -- Copying Data from an Azure Stage: https://docs.snowflake.com/en/user-guide/data-load-azure-copy
    -- COPY INTO <table>: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table

COPY INTO sales_bronze
  FROM @my_azure_stage
    FILES = ( 'Sales_1.csv')
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);



-------------------------------------------------------------------------------------------
    -- Step 8: Start querying your Data!
-------------------------------------------------------------------------------------------

---> Great job! You just successfully loaded data from your cloud provider into a Snowflake table
---> through an external stage. You can now start querying or analyzing the data.

SELECT * FROM sales_bronze;
