create or replace function bitemporal_internal.ll_bitemporal_update_select_keys(
    p_schema_name text,
    p_table_name text,
    p_list_of_fields text, -- fields to update
    p_values_selected_update text,  -- values to update with
    p_keys integer[], --the keys for the rows that are to be updated
    p_effective temporal_relationships.timeperiod,  -- effective range of the update
    p_asserted temporal_relationships.timeperiod  -- assertion for the update
)
returns integer[] --the keys of the updated rows
language plpgsql
as $body$
declare
    v_list_of_fields_to_insert text:=' ';
    v_list_of_fields_to_insert_excl_effective text;
    v_table_attr text[];
    v_serial_key text:=p_table_name||'_key';
    v_table text:=p_schema_name||'.'||p_table_name;
    v_keys_upd integer[];
    v_now timestamptz:=now();-- so that we can reference this time
    v_list_of_fields text;
    v_list_of_fields_final_insert text := '';
    v_field text;
begin
    if lower(p_asserted)<v_now::date --should we allow this precision?...
        or upper(p_asserted)< 'infinity'
    then
        raise exception'asserted interval starts in the past or has a finite end: %', p_asserted;
    end if;

    v_table_attr := bitemporal_internal.ll_bitemporal_list_of_fields(v_table);
    if array_length(v_table_attr,1)=0 then
        raise exception 'Empty list of fields for a table: %', v_table;
    end if;

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
    EXECUTE format($u$
        WITH updt AS (
            UPDATE %1$s t
            SET asserted = temporal_relationships.timeperiod(
                lower(asserted),
                lower(%2$L::temporal_relationships.timeperiod)
            )
            WHERE %3$s = any(%4$L)
            AND (
                temporal_relationships.is_overlaps(effective, %5$L)
                OR temporal_relationships.is_meets(effective::temporal_relationships.timeperiod, %5$L)
                OR temporal_relationships.has_finishes(effective::temporal_relationships.timeperiod, %5$L)
            )
            AND lower(%2$L::temporal_relationships.timeperiod)<@ asserted
            returning %3$s
        )
        SELECT array_agg(%3$s) FROM updt
        $u$,
        v_table,      --%1
        p_asserted,   --%2
        v_serial_key, --%3
        p_keys,       --%4
        p_effective   --%5
    )
    into v_keys_upd;

    if v_keys_upd is null then
        return v_keys_upd;
    end if;

    --insert new assertion range with old values and effective-ended
    EXECUTE format($i$
        INSERT INTO %1$s ( %2$s, effective, asserted )
        SELECT %2$s ,temporal_relationships.timeperiod(lower(effective), lower(%3$L::temporal_relationships.timeperiod)) ,%4$L
        FROM %1$s
        WHERE %5$s = any(%6$L)
        $i$,
        v_table,
        v_list_of_fields_to_insert_excl_effective,
        p_effective,
        p_asserted,
        v_serial_key,
        v_keys_upd
    );


    --insert new assertion range with new values and new effective range
    --(combined insert and update to decrease I/O)
    EXECUTE format($i$
        INSERT INTO %1$s ( %2$s, effective, asserted )
        SELECT %3$s ,%4$L, %5$L
        FROM %1$s t
        cross join lateral (%6$s) nv(%7$s)
        WHERE %8$s = any(%9$L)
        $i$,
        v_table,
        v_list_of_fields_to_insert_excl_effective,
        v_list_of_fields_final_insert,
        p_effective,
        p_asserted,
        p_values_selected_update,
        p_list_of_fields,
        v_serial_key,
        v_keys_upd
    );

    return v_keys_upd;
end;
$body$;
