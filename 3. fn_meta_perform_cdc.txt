create or replace procedure fn_meta_perform_cdc(TGT_DB_TABLE_NAME string, LOAD_TYPE string)
  returns string not null
  language javascript
  EXECUTE AS CALLER
  as     
  $$
	try
	{
        // Fetch target table column names
        var tgt_col_list_cmd           =    "select COLUMN_NAME, COALESCE(COMMENT,'') from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = '" + TGT_DB_TABLE_NAME.split('.')[0] + "' and table_name = '" + TGT_DB_TABLE_NAME.split('.')[1] + "' order by ordinal_position";
        var statement                  =    snowflake.createStatement( {sqlText: tgt_col_list_cmd} );
        var tgt_col_list_result        =    statement.execute();
	    
	    var tgt_col_list = []
	    while(tgt_col_list_result.next())
	    	tgt_col_list.push([tgt_col_list_result.getColumnValue(1), tgt_col_list_result.getColumnValue(2)])
        
        // Get All Columns List
	    var pk_col_list     =    tgt_col_list.filter(col_value => col_value[1].toUpperCase().includes('PRIMARY KEY')).map(function(col_value) {return col_value[0]})
	    var meta_col_list   =    tgt_col_list.filter(col_value => col_value[0].startsWith("META_")).map(function(col_value) {return col_value[0]})
	    var non_pk_col_list =    tgt_col_list.filter(col_value => !(pk_col_list.includes(col_value[0]) || meta_col_list.includes(col_value[0]))).map(function(col_value) {return col_value[0]})
	    var tgt_col_list	=	 tgt_col_list.map(function(col_value) {return col_value[0]})
	    
        // Define meta-data replacement strings
        var meta_columns_dict = {
	    							"INSERT"	:	{
	    												"META_REC_SRCE_BUS_NAME"	:   "ID.META_REC_SRCE_BUS_NAME"
	    												,"META_REC_BATCH_ID"		:   "ID.META_REC_BATCH_ID"
	    												,"META_REC_START_DATE_TIME"	:   "TIMESTAMPADD(second,1,$ts_start)"
	    												,"META_REC_END_DATE_TIME"  	:   "NULL"
	    												,"META_REC_STATUS_IND"     	:   "CASE\n    WHEN " + pk_col_list.map(function(col_name) {return "TGT." + col_name + " IS NULL"}).join("AND") + " THEN 'New'\n    WHEN " + pk_col_list.map(function(col_name) {return "ID."  + col_name + " IS NULL"}).join("AND") + " THEN 'Delete'\n    WHEN TGT.META_REC_STATUS_IND <>'Delete' AND (" + non_pk_col_list.map(function(col_name) {return "ID."  + col_name + " = TGT." + col_name}).join("AND") + ") THEN 'Ignore'\n    ELSE 'Change'\n   END"
	    												,"META_REC_ACTIVE_IND"		:	"'Active'"
	    											},
	    							"UPDATE"	:	{
	    												"META_REC_SRCE_BUS_NAME"	:   "TGT.META_REC_SRCE_BUS_NAME"
	    												,"META_REC_BATCH_ID"		:   "TGT.META_REC_BATCH_ID"
	    												,"META_REC_START_DATE_TIME"	:   "TGT.META_REC_START_DATE_TIME"
	    												,"META_REC_END_DATE_TIME"  	:   "$ts_start"
	    												,"META_REC_STATUS_IND"     	:   "TGT.META_REC_STATUS_IND"
	    												,"META_REC_ACTIVE_IND"		:	"TGT.META_REC_ACTIVE_IND"
	    											}
	    						}
	    
	    // ID Table Name
	    ID_DB_TABLE_NAME 			= TGT_DB_TABLE_NAME.replace("DWH_CORE.","DWH_STAGE.") + "_ID";
	    
	    // Setting start time of session
	    var start_time_query = "set ts_start = (select current_timestamp())";
	    var statement = snowflake.createStatement( {sqlText: start_time_query} );
	    statement.execute();
	    
	    if(LOAD_TYPE == "TRUNCATE LOAD")
	    {
	    	var truncate_query = "TRUNCATE TABLE " + TGT_DB_TABLE_NAME;
	    	var statement = snowflake.createStatement( {sqlText: truncate_query} );
	    	statement.execute();
	    }
        
        // Creating temp table
	    var temp_table_name			=	TGT_DB_TABLE_NAME.replace("DWH_CORE.","DWH_STAGE.") + "_TEMP"
	    var temp_table_query 		= "CREATE OR REPLACE TEMPORARY TABLE " + temp_table_name + " AS\n" + 
	    							"WITH\n  " + TGT_DB_TABLE_NAME.split(".")[1] + " AS\n  (\n    SELECT *\n    FROM " + 
	    								TGT_DB_TABLE_NAME + " TGT\n    WHERE META_REC_ACTIVE_IND = 'Active'\n    AND EXISTS (SELECT 1 FROM " + 
	    								ID_DB_TABLE_NAME + " ID WHERE ID.META_REC_SRCE_BUS_NAME = TGT.META_REC_SRCE_BUS_NAME)\n" +
	    							"  )\nSELECT\n  " + 
	    							[
	    								pk_col_list.map(function(col_name) {return "ID." + col_name + " AS " + col_name + "_ID"}).join("\n  ,"),
	    								non_pk_col_list.map(function(col_name) {return "ID." + col_name + " AS " + col_name + "_ID"}).join("\n  ,"),
	    								meta_col_list.map(function(col_name) {return meta_columns_dict["INSERT"][col_name] + " AS " + col_name + "_ID"}).join("\n  ,")
	    							].join("\n  ,") + "\n  ," +
	    							[
	    								pk_col_list.map(function(col_name) {return "TGT." + col_name + " AS " + col_name + "_TGT"}).join("\n  ,"),
	    								non_pk_col_list.map(function(col_name) {return "TGT." + col_name + " AS " + col_name + "_TGT"}).join("\n  ,"),
	    								meta_col_list.map(function(col_name) {return meta_columns_dict["UPDATE"][col_name] + " AS " + col_name + "_TGT"}).join("\n  ,")
	    							].join("\n  ,") +
	    							"\nFROM\n  " + TGT_DB_TABLE_NAME.split(".")[1] + " TGT\n  FULL OUTER JOIN\n    " + ID_DB_TABLE_NAME + " ID\n    ON " + 
	    								pk_col_list.map(function(col_name) {return "ID." + col_name + " = TGT." + col_name}).join(" AND\n      ") 
	    var statement = snowflake.createStatement( {sqlText: temp_table_query} );
	    statement.execute();
	    
	    // Upsert New/Change/Delete records as Active in Target table
	    var active_insert_query		= "MERGE INTO " + TGT_DB_TABLE_NAME + " TGT\n" +
	    								"USING " + temp_table_name + " TEMP\n" +
	    								  "ON\n" + pk_col_list.map(function(col_name) {return "TEMP." + col_name + "_TGT = TGT." + col_name}).join("\n  AND") +
	    								    "\nWHEN MATCHED AND TEMP.META_REC_ACTIVE_IND_TGT = 'Active' AND TEMP.META_REC_STATUS_IND_ID NOT IN ('Ignore', 'Delete')\n  THEN UPDATE SET \n     " +
	    									  tgt_col_list.map(function(col_name) {return "TGT." + col_name + " = " + ((col_name == "META_REC_ACTIVE_IND") ? "'Active'" :  (col_name == "META_REC_START_DATE_TIME" ? "TEMP.META_REC_START_DATE_TIME_ID" : "TEMP." + col_name + "_ID"))}).join("\n    ,") + 
	    									    "\nWHEN MATCHED AND TEMP.META_REC_STATUS_IND_ID = 'Delete'\n  THEN UPDATE SET \n     " +
	    									      tgt_col_list.map(function(col_name) {return "TGT." + col_name + " = " + ((col_name == "META_REC_ACTIVE_IND") ? "'Active'" : (col_name == "META_REC_START_DATE_TIME" ? "TEMP.META_REC_START_DATE_TIME_ID" : ((col_name == "META_REC_END_DATE_TIME") ? "NULL" : "TEMP." + col_name + "_TGT")))}).join("\n    ,") + 
	    									        "\nWHEN NOT MATCHED\n  THEN INSERT (" + tgt_col_list.join(", ") + ")" +
	    									          "\n    VALUES(" + tgt_col_list.map(function(col_name) {return "TEMP." + col_name + "_ID"}).join(", ") + ")"
	    var statement = snowflake.createStatement( {sqlText: active_insert_query} );
	    statement.execute();
	    
	    if(LOAD_TYPE == "Type 2")
	    {
	    	// Insert Change/Delete records as Inactive in Target table
	    	var inactive_insert_query	= "INSERT INTO " + TGT_DB_TABLE_NAME + "\n" + 
	    									"SELECT\n  " + 
	    									[
	    										pk_col_list.map(function(col_name) {return "TEMP." + col_name + "_TGT"}).join("\n  ,"),
	    										non_pk_col_list.map(function(col_name) {return "TEMP." + col_name + "_TGT"}).join("\n  ,"),
	    										meta_col_list.map(function(col_name) {return ((col_name == "META_REC_ACTIVE_IND") ? "'Inactive'" : ((col_name == "META_REC_START_DATE_TIME") ? "META_REC_END_DATE_TIME_TGT" : ((col_name == "META_REC_END_DATE_TIME") ? "TEMP.META_REC_END_DATE_TIME_TGT" : "TEMP." + col_name + "_TGT")))}).join("\n  ,")
	    									].join("\n  ,") +
	    									"\nFROM\n  " + temp_table_name + " TEMP\nWHERE\n  TEMP.META_REC_STATUS_IND_ID IN ('Change', 'Delete')"
	    	var statement = snowflake.createStatement( {sqlText: inactive_insert_query} );
	    	statement.execute();
	    }
	}
	catch(err)
	{
		return err.code
	}
	
	return 0;
  $$
  ;