-- NOTE: this test cannot use 'current_database = currentDatabase()',
-- because it does not propagated via remote queries,
-- hence it uses 'with (select currentDatabase()) as X'
-- (with subquery to expand it on the initiator).

set log_queries=0;
set log_queries_min_type='QUERY_FINISH';

drop table if exists data_01814;
drop table if exists dist_01814;

create table data_01814 (key Int) Engine=MergeTree() order by key settings index_granularity=10 as select * from numbers(100);

-- NOTES:
-- - max_rows_to_read_leaf cannot be used since it does not know anything about optimize_aggregation_in_order,
-- - limit push down can be checked only with optimize_aggregation_in_order, since otherwise the query will be canceled too early, and read_rows will be small.
select 'distributed_push_down_limit=1';
with (select currentDatabase()) as id_distributed_push_down_limit_1
select *
from remote('127.{2,3}', currentDatabase(), data_01814)
group by key
limit 10
settings
    log_queries=1,
    optimize_aggregation_in_order=1,
    max_block_size=20,
    distributed_push_down_limit=1
format Null;
system flush logs;
select read_rows
from system.query_log
where
    event_date = today() and
    query_kind = 'Select' and
    query not like '%system.query_log%' and
    query ilike concat('WITH%', currentDatabase(), '%AS id_distributed_push_down_limit_1 %')
;

select 'distributed_push_down_limit=0';
with (select currentDatabase()) as id_distributed_push_down_limit_0
select *
from remote('127.{2,3}', currentDatabase(), data_01814)
group by key
limit 10
settings
    log_queries=1,
    optimize_aggregation_in_order=1,
    max_block_size=20,
    distributed_push_down_limit=0
format Null;
system flush logs;
select read_rows
from system.query_log
where
    event_date = today() and
    query_kind = 'Select' and
    query not like '%system.query_log%' and
    query ilike concat('WITH%', currentDatabase(), '%AS id_distributed_push_down_limit_0 %')
;

select 'auto-distributed_push_down_limit';
create table dist_01814 as data_01814 engine=Distributed('test_cluster_two_shards', currentDatabase(), data_01814, key);
with (select currentDatabase()) as id_auto_distributed_push_down_limit
select *
from dist_01814
group by key
limit 10
settings
    log_queries=1,
    optimize_skip_unused_shards=1,
    optimize_distributed_group_by_sharding_key=1,
    optimize_aggregation_in_order=1,
    max_block_size=20,
    prefer_localhost_replica=0,
    distributed_push_down_limit=0
format Null;
system flush logs;
select read_rows
from system.query_log
where
    event_date = today() and
    query_kind = 'Select' and
    query not like '%system.query_log%' and
    query ilike concat('WITH%', currentDatabase(), '%AS id_auto_distributed_push_down_limit %')
;

drop table data_01814;
drop table dist_01814;
