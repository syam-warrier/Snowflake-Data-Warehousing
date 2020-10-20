create or replace procedure fn_meta_close_batch(BATCH_ID string, BATCH_STATUS string)
  returns float not null
  language javascript
  as     
  $$
    try
    {
        // Insert new batch id details in META_BATCH_DETAILS table
        var insert_batch_close_details_cmd  =    "INSERT INTO DWH_META.META_BATCH_DETAILS SELECT BATCH_ID, APPLICATION_NAME, BATCH_MODE, DATA_WINDOW_OPEN_TS, DATA_WINDOW_CLOSE_TS, :1, BATCH_STATUS_DESCRIPTION, CURRENT_TIMESTAMP::TIMESTAMP_NTZ from DWH_META.META_BATCH_DETAILS WHERE BATCH_ID=:2;";
        var statement                       =    snowflake.createStatement( {sqlText: insert_batch_close_details_cmd, binds:[BATCH_STATUS, BATCH_ID]} );
        var insert_batch_close_status       =    statement.execute();

        return 0;
     }
     catch (err)
     {
        return err.code;
     }
     
  $$
  ;