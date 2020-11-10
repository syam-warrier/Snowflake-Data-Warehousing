create or replace procedure fn_meta_open_batch(APPLICATION_NAME string, BATCH_MODE string)
  returns float not null
  language javascript
  execute as caller
  as     
  $$  
    // Fetch latest batch-id in META_BATCH_DETAILS table
    var get_prev_batch_details_cmd      =   "select TO_VARCHAR(SYSDATE(), 'YYYYMMDDHHMISS')::INTEGER, COALESCE(max(case when APPLICATION_NAME = '" + APPLICATION_NAME + "' AND BATCH_MODE = '" + BATCH_MODE + "' AND BATCH_STATUS = 'SUCCESS' THEN DATA_WINDOW_CLOSE_TS END), TO_TIMESTAMP('1900-01-01 00:00:00')) from DWH_META.META_BATCH_DETAILS;";
    var statement                       =   snowflake.createStatement( {sqlText: get_prev_batch_details_cmd} );
    var prev_batch_details              =   statement.execute();
    prev_batch_details.next();
    
    var new_batch_id                    =   prev_batch_details.getColumnValue(1);
    var prev_success_batch_close_ts     =   prev_batch_details.getColumnValue(2);
    
    // Calculate new batch parameters
    var batch_status                    =   'OPEN';
    var batch_status_description        =   'Opening new ' + BATCH_MODE + ' job for ' + APPLICATION_NAME + '.';
    
    // Insert new batch id details in META_BATCH_DETAILS table
    var insert_new_batch_details_cmd    =    "INSERT INTO DWH_META.META_BATCH_DETAILS VALUES (?,?,?,?,CURRENT_TIMESTAMP::TIMESTAMP_NTZ,?,?,CURRENT_TIMESTAMP::TIMESTAMP_NTZ)";
    var statement                       =    snowflake.createStatement( {sqlText: insert_new_batch_details_cmd, binds:[new_batch_id, APPLICATION_NAME, BATCH_MODE, prev_success_batch_close_ts, batch_status, batch_status_description]} );
    var insert_new_batch_id_status      =    statement.execute();
    
    return new_batch_id; 
  $$
  ;