create or replace procedure fn_meta_merge_stage_from_landing(BATCH_ID string, STAGE_SCHEMA_TABLE_NAME string)
  returns string not null
  language javascript
  as
  $$
    try
    {
        // Fetch stage table column names
        var stage_col_list_cmd         =    "DESC TABLE DWH_ACTV_STAGING." + STAGE_SCHEMA_TABLE_NAME;
        var statement                  =    snowflake.createStatement( {sqlText: stage_col_list_cmd} );
        var stage_col_list_res         =    statement.execute();
        
        // Get All Columns List
        var col_list = [], pk_col_list = [], non_pk_col_list = [];
        while (stage_col_list_res.next())
        {
            col_list.push([stage_col_list_res.getColumnValue(1), stage_col_list_res.getColumnValue(2)]);
            // Segregate PK and non-PK Columns List
            if(stage_col_list_res.getColumnValue(6) == 'Y')
                pk_col_list.push([stage_col_list_res.getColumnValue(1), stage_col_list_res.getColumnValue(2)]);
            else
                non_pk_col_list.push([stage_col_list_res.getColumnValue(1), stage_col_list_res.getColumnValue(2)]);
         }
        
        // Define meta-data replacement strings
        var meta_columns_dict = {
        "INSERT" :  {
                    "META_REC_SRCE_BUS_NME"   : "'<BUSINESSNAME>'",
                    "META_REC_SRCE_FILE_NAME" : "B.META_REC_SRCE_FILE_NAME",
                    "META_REC_SRCE_ROW_NUM"   : "B.META_REC_SRCE_ROW_NUM",
                    "META_REC_BATCH_ID"       : BATCH_ID,
                    "META_REC_CRE_DATE_TIME"  : "SYSDATE()",
                    "META_REC_UPD_DATE_TIME"  : "NULL"
                    },
        "UPDATE" :  {
                    "META_REC_SRCE_BUS_NME"   : "'<BUSINESSNAME>'",
                    "META_REC_SRCE_FILE_NAME" : "B.META_REC_SRCE_FILE_NAME",
                    "META_REC_SRCE_ROW_NUM"   : "B.META_REC_SRCE_ROW_NUM",
                    "META_REC_BATCH_ID"       : BATCH_ID,
                    "META_REC_CRE_DATE_TIME"  : "A.META_REC_CRE_DATE_TIME",
                    "META_REC_UPD_DATE_TIME"  : "SYSDATE()"
                    }
        }
        
        // Generate JOIN condition
        join_condition = pk_col_list.map(function(column) {return "\n\tA." + column[0] + " = B." + column[0] + " :: " + column[1]}).join(" AND ");
        
        // Generate UPDATE columns list
        update_cols_part = non_pk_col_list.map(function(column) {return "\n\tA." + column[0] + " = " + (Object.keys(meta_columns_dict["UPDATE"]).includes(column[0]) ? meta_columns_dict["UPDATE"][column[0]] : "B." + column[0] + " :: " + column[1]) }).join();
        
        // Generate INSERT column values list
        ins_cols_part = col_list.map(function(column) { return (Object.keys(meta_columns_dict["INSERT"]).includes(column[0]) ? meta_columns_dict["INSERT"][column[0]] : "B." + column[0] + " :: " + column[1]) }).join();
        
        // Generate Query for merging landing table's data into active stage
        var query = "MERGE INTO DWH_ACTV_STAGING." + STAGE_SCHEMA_TABLE_NAME + " A\nUSING DWH_LANDING." + STAGE_SCHEMA_TABLE_NAME + " B\nON" + join_condition + "\nWHEN MATCHED THEN UPDATE SET" + update_cols_part + "\n WHEN NOT MATCHED THEN INSERT (" + col_list.map(function(col) {return col[0]}).join(',') + ") VALUES (" + ins_cols_part + ");";
        var statement = snowflake.createStatement( {sqlText: query} );
        statement.execute()
        
        // Generate Query for merging landing table's data into active stage
        var query = "INSERT INTO DWH_DATA_LAKE." + STAGE_SCHEMA_TABLE_NAME + "(" + col_list.map(function(col) {return col[0]}).join(',') + ")\nSELECT " + ins_cols_part + " FROM DWH_LANDING." + STAGE_SCHEMA_TABLE_NAME + " B" ;
        var statement = snowflake.createStatement( {sqlText: query} );
        statement.execute()
        
        return 0;
    }
    catch (err)
    {
       return err.code;
    }
  $$
  ;