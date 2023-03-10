CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_update_select(p_table text
,p_list_of_fields text -- fields to update
,p_values_selected_update TEXT  -- values to update with
,p_search_fields TEXT  -- search fields
,p_values_selected_search TEXT  --  search values selected
,p_effective temporal_relationships.timeperiod  -- effective range of the update
,p_asserted temporal_relationships.timeperiod  -- assertion for the update
) 
RETURNS INTEGER
AS
$BODY$
DECLARE
v_rowcount INTEGER:=0;
v_list_of_fields_to_insert text:=' ';
v_list_of_fields_to_insert_excl_effective text;
v_table_attr text[];
v_now timestamptz:=now();-- so that we can reference this time
BEGIN 
 IF lower(p_asserted)<v_now::date --should we allow this precision?...
    OR upper(p_asserted)< 'infinity'
 THEN RAISE EXCEPTION'Asserted interval starts in the past or has a finite end: %', p_asserted
  ; 
  RETURN v_rowcount;
 END IF;
v_table_attr := bitemporal_internal.ll_bitemporal_list_of_fields(p_table);
IF  array_length(v_table_attr,1)=0
      THEN RAISE EXCEPTION 'Empty list of fields for a table: %', p_table; 
  RETURN v_rowcount;
 END IF;
v_list_of_fields_to_insert_excl_effective:= array_to_string(v_table_attr, ',','');
v_list_of_fields_to_insert:= v_list_of_fields_to_insert_excl_effective||',effective';

--end assertion period for the old record(s)

EXECUTE format($u$ UPDATE %s t    SET asserted =
            temporal_relationships.timeperiod(lower(asserted), lower(%L::temporal_relationships.timeperiod))
                    WHERE ( %s )in( %s ) AND (temporal_relationships.is_overlaps(effective, %L)
                                       OR 
                                       temporal_relationships.is_meets(effective::temporal_relationships.timeperiod, %L)
                                       OR 
                                       temporal_relationships.has_finishes(effective::temporal_relationships.timeperiod, %L))
                                      AND lower(%L::temporal_relationships.timeperiod)<@ asserted  $u$
                                      --changed now() to lower(%L::temporal_relationships.timeperiod)
                                      --in the line above this comment to handle the more generic case of
                                      --using a timeperiod that doesn't start with now()
          , p_table
          , p_asserted
          , p_search_fields
          , p_values_selected_search
          , p_effective
          , p_effective
          , p_effective
          , p_asserted);

 --insert new assertion rage with old values and effective-ended
EXECUTE format($i$INSERT INTO %s ( %s, effective, asserted )
                SELECT %s ,temporal_relationships.timeperiod(lower(effective), lower(%L::temporal_relationships.timeperiod)) ,%L
                  FROM %s WHERE ( %s )in( %s ) AND (temporal_relationships.is_overlaps(effective, %L)
                                       OR 
                                       temporal_relationships.is_meets(effective, %L)
                                       OR 
                                       temporal_relationships.has_finishes(effective, %L))
                                      AND upper(asserted)=lower(%L::temporal_relationships.timeperiod) $i$
          , p_table
          , v_list_of_fields_to_insert_excl_effective
          , v_list_of_fields_to_insert_excl_effective
          , p_effective
          , p_asserted
          , p_table
          , p_search_fields
          , p_values_selected_search
          , p_effective
          , p_effective
          , p_effective
          , p_asserted
);


---insert new assertion rage with old values and new effective range
 
EXECUTE format($i$INSERT INTO %s ( %s, effective, asserted )
                SELECT %s ,%L, %L
                  FROM %s WHERE ( %s )in( %s ) AND (temporal_relationships.is_overlaps(effective, %L)
                                       OR 
                                       temporal_relationships.is_meets(effective, %L)
                                       OR 
                                       temporal_relationships.has_finishes(effective, %L))
                                      AND upper(asserted)=lower(%L::temporal_relationships.timeperiod) $i$
          , p_table
          , v_list_of_fields_to_insert_excl_effective
          , v_list_of_fields_to_insert_excl_effective
          , p_effective
          , p_asserted
          , p_table
          , p_search_fields
          , p_values_selected_search
          , p_effective
          , p_effective
          , p_effective
          , p_asserted
);

--update new record(s) in new assertion rage with new values                                  
                                  
EXECUTE format($u$ UPDATE %s t SET (%s) = (%s) 
                    WHERE ( %s ) in ( %s ) AND effective=%L
                                        AND asserted=%L $u$  
          , p_table
          , p_list_of_fields
          , p_values_selected_update
          , p_search_fields
          , p_values_selected_search
          , p_effective
          , p_asserted);
          
GET DIAGNOSTICS v_rowcount:=ROW_COUNT;  
RETURN v_rowcount;
END;    
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_update_select(p_schema_name text,
 p_table_name  text
,p_list_of_fields text -- fields to update
,p_values_selected_update TEXT  -- values to update with
,p_search_fields TEXT  -- search fields
,p_values_selected_search TEXT  --  search values selected
,p_effective temporal_relationships.timeperiod  -- effective range of the update
,p_asserted temporal_relationships.timeperiod  -- assertion for the update
) 
RETURNS INTEGER
AS
$BODY$
DECLARE
v_rowcount INTEGER:=0;
v_list_of_fields_to_insert text:=' ';
v_list_of_fields_to_insert_excl_effective text;
v_table_attr text[];
v_serial_key text:=p_table_name||'_key';
v_table text:=p_schema_name||'.'||p_table_name;
v_keys_old int[];
v_keys int[];
v_now timestamptz:=now();-- so that we can reference this time
v_list_of_fields text;
v_list_of_fields_final_insert text := '';
v_field text;
BEGIN 
 IF lower(p_asserted)<v_now::date --should we allow this precision?...
    OR upper(p_asserted)< 'infinity'
 THEN RAISE EXCEPTION'Asserted interval starts in the past or has a finite end: %', p_asserted
  ; 
  RETURN v_rowcount;
 END IF;
v_table_attr := bitemporal_internal.ll_bitemporal_list_of_fields(v_table);
IF  array_length(v_table_attr,1)=0
      THEN RAISE EXCEPTION 'Empty list of fields for a table: %', v_table; 
  RETURN v_rowcount;
 END IF;
v_list_of_fields_to_insert_excl_effective:= array_to_string(v_table_attr, ',','');
v_list_of_fields_to_insert:= v_list_of_fields_to_insert_excl_effective||',effective';

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

--end assertion period for the old record(s)

EXECUTE format($u$ WITH updt AS (UPDATE %s t SET asserted =
            temporal_relationships.timeperiod(lower(asserted), lower(%L::temporal_relationships.timeperiod))
                    WHERE ( %s )in( %s ) AND (temporal_relationships.is_overlaps(effective, %L)
                                       OR 
                                       temporal_relationships.is_meets(effective::temporal_relationships.timeperiod, %L)
                                       OR 
                                       temporal_relationships.has_finishes(effective::temporal_relationships.timeperiod, %L))
                                      AND lower(%L::temporal_relationships.timeperiod)<@ asserted returning %s )
                                      SELECT array_agg(%s) FROM updt
                                      $u$  
                                      --changed now() to lower(%L::temporal_relationships.timeperiod)
                                      --in the line 3 lines above this comment to handle the more generic case of
                                      --using a timeperiod that doesn't start with now()
          , v_table
          , p_asserted
          , p_search_fields
          , p_values_selected_search
          , p_effective
          , p_effective
          , p_effective
          , p_asserted
          , v_serial_key
          , v_serial_key) into v_keys_old;
  if v_keys_old is null then 
  return 0;
end if;        

 --insert new assertion rage with old values and effective-ended
EXECUTE format($i$INSERT INTO %s ( %s, effective, asserted )
                SELECT %s ,temporal_relationships.timeperiod(lower(effective), lower(%L::temporal_relationships.timeperiod)) ,%L
                  FROM %s WHERE ( %s )in( %s )
                                       $i$
          , v_table
          , v_list_of_fields_to_insert_excl_effective
          , v_list_of_fields_to_insert_excl_effective
          , p_effective
          , p_asserted
          , v_table
          , v_serial_key
          ,array_to_string(v_keys_old,',')
);


---insert new assertion range with new values and new effective range
--(combined insert and update to decrease I/O)
EXECUTE format($i$ INSERT INTO %s ( %s, effective, asserted )
                  SELECT %s ,%L, %L
                  FROM %s t
                  cross join lateral (%s) nv(%s)
                  WHERE (%s) in (%s)
                $i$
          , v_table
          , v_list_of_fields_to_insert_excl_effective
          , v_list_of_fields_final_insert
          , p_effective
          , p_asserted
          , v_table
          , p_values_selected_update
          , p_list_of_fields
          , v_serial_key
          , array_to_string(v_keys_old,',')
);

GET DIAGNOSTICS v_rowcount:=ROW_COUNT;  
RETURN v_rowcount;
END;    
$BODY$ LANGUAGE plpgsql;



