create or replace procedure fn_meta_upd_stage_from_pre_stage(PRE_STAGE_DB_TABLE_NAME string, STAGE_DB_TABLE_NAME string)
  returns string not null
  language javascript
  as     
  $$  
    // Fetch pre-stage table column names
    var pre_stage_col_list_cmd     =    "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = '" + PRE_STAGE_DB_TABLE_NAME.split('.')[0] + "' and table_name = '" + PRE_STAGE_DB_TABLE_NAME.split('.')[1] + "' order by ordinal_position";
    var statement                  =    snowflake.createStatement( {sqlText: pre_stage_col_list_cmd} );
    var pre_stage_col_list_res     =    statement.execute();
    
    // Fetch stage table column names
    var stage_col_list_cmd         =    "select COLUMN_NAME, COMMENT from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = '" + STAGE_DB_TABLE_NAME.split('.')[0] + "' and table_name = '" + STAGE_DB_TABLE_NAME.split('.')[1] + "' order by ordinal_position";
    var statement                  =    snowflake.createStatement( {sqlText: stage_col_list_cmd} );
    var stage_col_list_res         =    statement.execute();
    
    // Compare column names for pre-stage and stage tables
    
    // Get All Columns List
    var col_list = [], pk_col_list = [], non_pk_col_list = [];
    while (stage_col_list_res.next())
    {
        col_list.push(stage_col_list_res.getColumnValue(1));
        // Segregate PK and non-PK Columns List
        if(stage_col_list_res.getColumnValue(2) == 'PRIMARY KEY')
            pk_col_list.push(stage_col_list_res.getColumnValue(1));
        else
            non_pk_col_list.push(stage_col_list_res.getColumnValue(1));
     }
    
    // Define meta-data replacement strings
    var meta_columns_dict = {
    "INSERT" :  {
                "META_REC_SRCE_BUS_NME"   : "'<BUSINESSNAME>'",
                "META_REC_SRCE_FILE_NAME" : "B.META_REC_SRCE_FILE_NAME",
                "META_REC_SRCE_ROW_NUM"   : "B.META_REC_SRCE_ROW_NUM",
                "META_REC_BATCH_ID"       : "0",
                "META_REC_CRE_DATE_TIME"  : "SYSDATE()",
                "META_REC_UPD_DATE_TIME"  : "NULL"
                },
    "UPDATE" :  {
                "META_REC_SRCE_BUS_NME"   : "'<BUSINESSNAME>'",
                "META_REC_SRCE_FILE_NAME" : "B.META_REC_SRCE_FILE_NAME",
                "META_REC_SRCE_ROW_NUM"   : "B.META_REC_SRCE_ROW_NUM",
                "META_REC_BATCH_ID"       : "0",
                "META_REC_CRE_DATE_TIME"  : "A.META_REC_CRE_DATE_TIME",
                "META_REC_UPD_DATE_TIME"  : "SYSDATE()"
                }
    }
    
    // Generate JOIN condition
    join_condition = pk_col_list.map(function(column) {return "\n\tA." + column + " = B." + column + ","}).join();
    join_condition = join_condition.substr(0, join_condition.length - 1);
    
    // Generate UPDATE columns list
    update_cols_part = non_pk_col_list.map(function(column) {return "\n\tA." + column + " = " + (Object.keys(meta_columns_dict["UPDATE"]).includes(column) ? meta_columns_dict["UPDATE"][column] : "B." + column) }).join();
    
    // Generate INSERT column values list
    ins_cols_part = col_list.map(function(column) { return (Object.keys(meta_columns_dict["INSERT"]).includes(column) ? meta_columns_dict["INSERT"][column] : "B." + column) }).join();
    
    // Generate Query
    var query = "MERGE INTO " + STAGE_DB_TABLE_NAME + " A\nUSING " + PRE_STAGE_DB_TABLE_NAME + " B\nON" + join_condition + "\nWHEN MATCHED THEN UPDATE SET" + update_cols_part + "\n WHEN NOT MATCHED THEN INSERT (" + col_list.join(',') + ") VALUES (" + ins_cols_part + ");";
    var statement = snowflake.createStatement( {sqlText: query} );
    
  return statement.execute(); 
  $$
  ;