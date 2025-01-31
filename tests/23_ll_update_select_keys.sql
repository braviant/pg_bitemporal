
BEGIN;
set client_min_messages to warning;
set local search_path = 'bi_temp_tables','bitemporal_internal','public';
set local TimeZone  = 'UTC';

SELECT plan(13);

select  unialike( current_setting('search_path'), '%temporal_relationships%'
  ,'temporal_relationships should NOT be on search_path for these tests' );



select lives_ok($$
    create schema bi_temp_tables
$$, 'create schema');

select lives_ok($$
  create table bi_temp_tables.devices_manual (
      device_id_key serial NOT NULL
    , device_id integer NOT NULL
    , effective tstzrange
    , asserted tstzrange
    , device_descr text
    , row_created_at timestamptz NOT NULL DEFAULT now()
    , CONSTRAINT devices_device_id_asserted_effective_excl EXCLUDE
      USING gist (device_id WITH =, asserted WITH &&, effective WITH &&)
  )
$$, 'create devices manual');

select lives_ok($$select * from bitemporal_internal.ll_create_bitemporal_table('bi_temp_tables','devices',
'device_id integer, device_descr text', 'device_id')
$$, 'create devices');

select lives_ok($$
insert into  bi_temp_tables.devices( device_id , effective, asserted, device_descr )
values
 (1, '[01-01-2015, infinity)', '[01-01-2015, infinity)','descr2')
,(5, '[2015-01-01 00:00:00-06,infinity)', '[2015-01-01 00:00:00-06,infinity)', 'test_5')
$$, 'insert data into devices');

----test insert:
 select results_eq ($q$
  select bitemporal_internal.ll_bitemporal_insert('bi_temp_tables.devices',
  'device_id , device_descr', $$'11', 'new_descr'$$, '[01-01-2016, infinity)', '[01-02-2016, infinity)' )
$q$,
$v$ values(1) $v$
,'bitemporal insert'
);

-- select * from bi_temp_tables.devices ;

select results_eq($q$ select device_id, device_descr, effective, asserted
from bi_temp_tables.devices where device_id =11 $q$
, $v$
values
( 11
,'new_descr'::text
  ,'["2016-01-01 00:00:00+00",infinity)'::temporal_relationships.timeperiod
  ,'["2016-01-02 00:00:00+00",infinity)'::temporal_relationships.timeperiod
)
$v$
,'bitemporal insert, returns select '
);

create temporary table pg_temp.temp_keys as
select devices_key from bi_temp_tables.devices where device_id=1
union all
values(-1);--include a non-existing key to test that it is not returned

--update_select - 1 row
select is(
    (
        select * from bitemporal_internal.ll_bitemporal_update_select_keys(
            'bi_temp_tables',
            'devices',
            'device_descr',
            $$select 'new descr 1'$$,
            (select array_agg(devices_key) from pg_temp.temp_keys),
            '[2018-01-01, infinity)',
            '[2116-01-01, infinity)'
        )
    ),
    (select array_agg(devices_key) from pg_temp.temp_keys where devices_key>0),
    'initial update_select - 1 row'
);


select results_eq($q$
  select device_descr
        from bi_temp_tables.devices where device_id = 1
         and effective='[2018-01-01, infinity)'
         and asserted='[2116-01-01, infinity)'
$q$
, $v$
values
('new descr 1'::text)
$v$
,'after initial update_select - 1 row'
);

select results_eq($q$
  select 0 as device_key,device_id,device_descr,effective,asserted,
    row_created_at
  from bi_temp_tables.devices where device_id = 1
  order by effective,asserted
$q$
, $v$
select * from unnest(array[
(0,1,'new descr 1','[2018-01-01, infinity)','[2116-01-01, infinity)', now())
,(0,1,'descr2','[01-01-2015, infinity)','[01-01-2015, 2116-01-01)',now())
,(0,1,'descr2','[01-01-2015, 2018-01-01)','[2116-01-01, infinity)',now())
]::bi_temp_tables.devices[])
order by effective,asserted
$v$
,'after initial update_select - 1 row'
);

drop table pg_temp.temp_keys;

create temporary table pg_temp.temp_keys as
select devices_key from bi_temp_tables.devices
where device_id in (1,5)
and '2019-01-01'::timestamptz<@effective
and '2119-01-01'::timestamptz<@asserted;

select is(
    (
        select * from bitemporal_internal.ll_bitemporal_update_select_keys(
            'bi_temp_tables',
            'devices',
            'device_descr',
            $$select descr from (values (1,'newer descr 1'),(5,'new test_5')) tt(id,descr)
where tt.id=t.device_id$$,
            (select array_agg(devices_key) from pg_temp.temp_keys),
            '[2019-01-01, infinity)',
            '[2119-01-01, infinity)'
        )
    ),
    (select array_agg(devices_key) from pg_temp.temp_keys where devices_key>0),
    'update_select - 2 rows'
);

select results_eq($q$
  select device_id,device_descr
        from bi_temp_tables.devices where device_id in (1,5)
         and effective='[2019-01-01, infinity)'
         and asserted='[2119-01-01, infinity)'
         order by device_id
$q$
, $v$
values
(1,'newer descr 1')
,(5,'new test_5')
$v$
,'after update_select - 2 rows'
);

select results_eq($q$
  select 0 as device_key,device_id,device_descr,effective,asserted,
    row_created_at
  from bi_temp_tables.devices where device_id in (1,5)
  order by device_id,effective,asserted
$q$
, $v$
select * from unnest(array[
(0,1,'newer descr 1','[2019-01-01, infinity)','[2119-01-01, infinity)', now())
,(0,1,'new descr 1','[2018-01-01, 2019-01-01)','[2119-01-01, infinity)', now())
,(0,1,'new descr 1','[2018-01-01, infinity)','[2116-01-01, 2119-01-01)', now())
,(0,1,'descr2','[01-01-2015, infinity)','[01-01-2015, 2116-01-01)',now())
,(0,1,'descr2','[01-01-2015, 2018-01-01)','[2116-01-01, infinity)',now())
,(0,5,'new test_5','[2019-01-01, infinity)','[2119-01-01, infinity)', now())
,(0,5,'test_5','[01-01-2015 00:00:00-06, infinity)','[01-01-2015 00:00:00-06, 2119-01-01)',now())
,(0,5,'test_5','[01-01-2015 00:00:00-06, 2019-01-01)','[2119-01-01, infinity)',now())
]::bi_temp_tables.devices[])
order by device_id,effective,asserted
$v$
,'after update_select - 2 rows'
);



SELECT * FROM finish();
ROLLBACK;
-- vim: set filetype=pgsql expandtab tabstop=2 shiftwidth=2:
