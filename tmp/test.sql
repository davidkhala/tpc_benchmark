create or replace procedure run_crv()
  returns float not null
  language javascript
  as     
  $$  
    // for every row v in view V corresponding to fact table F

    var sql_cmd = "select * from crv";
    var stmt = snowflake.createStatement( {sqlText: sql_cmd} );
    var result_set = stmt.execute();

    // get row v into local variable lv
    while (result_set.next())  {
       var column1 = result_set.getColumnValue(1);
       var column2 = result_set.getColumnValue(2);
       // Do something with the retrieved values...
       }
  return 0.0; // Replace with something more useful.
  $$
  ;