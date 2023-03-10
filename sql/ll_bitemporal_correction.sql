 
  CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_correction(p_schema_name text,
    p_table_name text,
    p_list_of_fields text,
    p_list_of_values text,
    p_search_fields text,
    p_search_values text,
    p_effective temporal_relationships.timeperiod,
    p_now temporal_relationships.time_endpoint )
  RETURNS integer AS
$BODY$
DECLARE
v_sql  text;
  v_rowcount INTEGER:=0;
  v_list_of_fields_to_insert text;
  v_table_attr text[];
  v_now temporal_relationships.time_endpoint:=p_now ;-- for compatiability with the previous version
  v_serial_key text:=p_table_name||'_key';
  v_table text:=p_schema_name||'.'||p_table_name;
  v_effective_start temporal_relationships.time_endpoint:=lower(p_effective) ;
  v_keys int[];
  v_keys_old  int[];
  v_list_of_fields text;
  v_list_of_fields_final_insert text := '';
  v_field text;
BEGIN
 v_table_attr := bitemporal_internal.ll_bitemporal_list_of_fields(v_table);
 IF  array_length(v_table_attr,1)=0
      THEN RAISE EXCEPTION 'Empty list of fields for a table: %', v_table; 
  RETURN v_rowcount;
 END IF;

 v_list_of_fields_to_insert:= array_to_string(v_table_attr, ',','');
 
--surround with commas and remove white space, then we can test for field
--surrounded by commas using like.
v_list_of_fields := ','||regexp_replace(p_list_of_fields,'[[:space:]]*','','g')||',';

--loop through all of the fields that will be inserted and prepend the proper
--alias, t. if the field is not being updated and nv. (new value) if it is
--being updated.
foreach v_field in array v_table_attr loop
    v_list_of_fields_final_insert := v_list_of_fields_final_insert
        || case when '' <> v_list_of_fields_final_insert then ',' else '' end
        || case
            when v_list_of_fields like '%,'||v_field||',%' then 'nv.'
            else 't.'
        end
        ||v_field;
end loop;

EXECUTE 
 format($u$ WITH updt AS (UPDATE %s SET asserted = temporal_relationships.timeperiod_range(lower(asserted), %L, '[)')
                    WHERE ( %s )=( %s ) AND  %L=lower(effective)
                          AND upper(asserted)='infinity' 
                          AnD lower(asserted)<%L returning %s )
                                      SELECT array_agg(%s) FROM updt
                                      $u$  --end assertion period for the old record(s), if any
          , v_table
          , v_now
          , p_search_fields
          , p_search_values
          , v_effective_start
          , v_now
          , v_serial_key
          , v_serial_key) into v_keys_old;
 --       raise notice 'sql%', v_sql;  


if coalesce(array_to_string(v_keys_old,',')) IS NULL 
   then 
EXECUTE   format($uu$UPDATE %s SET ( %s ) = (SELECT %s ) WHERE ( %s ) = ( %s )
                           AND effective = %L
                           AND upper(asserted)='infinity'
                            $uu$  --update new assertion rage with new values
          , v_table
          , p_list_of_fields
          , p_list_of_values
          , p_search_fields
          , p_search_values
          , p_effective
     ) 
	;

ELSE 
---insert new assertion range with new values and new effective range
--(combined insert and update to decrease I/O)
EXECUTE format($i$ INSERT INTO %s ( %s, effective, asserted )
                  SELECT %s ,effective, temporal_relationships.timeperiod_range(upper(asserted), 'infinity', '[)')
                  FROM %s t
                  cross join lateral (select %s) nv(%s)
                  WHERE (%s) in (%s)
                $i$
          , v_table
          , v_list_of_fields_to_insert
          , v_list_of_fields_final_insert
          , v_table
          , p_list_of_values
          , p_list_of_fields
          , v_serial_key
          , array_to_string(v_keys_old,',')
);
end if;

 GET DIAGNOSTICS v_rowcount:=ROW_COUNT; 

 RETURN v_rowcount;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE; 
  
 
 CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_correction(p_schema_name text,
    p_table_name text,
    p_list_of_fields text,
    p_list_of_values text,
    p_search_fields text,
    p_search_values text,
    p_effective temporal_relationships.timeperiod)
  RETURNS integer AS
  $BODY$
  declare v_rowcount int;
  begin
   select * into v_rowcount from  bitemporal_internal.ll_bitemporal_correction(p_schema_name ,
    p_table_name ,
    p_list_of_fields ,
    p_list_of_values ,
    p_search_fields ,
    p_search_values,
    p_effective ,
    clock_timestamp() );
    return v_rowcount;
    END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
 