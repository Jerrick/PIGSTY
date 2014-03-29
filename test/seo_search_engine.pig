-- War Horse: 基础数据计算引擎
-- Input parameter: date
-- Dependency: visitlogs, user_profile, class_method_map, spam_daily, task.config
register /hadoop/pig/lib/piggybank.jar
register /hadoop/pig/lib/hive-exec-0.8.1.jar
register /hadoop/pig/lib/hive-common-0.8.1.jar

DEFINE match_search_engine `match_search_engine.py` SHIP('match_search_engine.py', 'search_engine.conf');
DEFINE seo_compute `seo_compute.py` SHIP('seo_compute.py');
DEFINE seo_stat_metrics `seo_stat_metrics.py` SHIP('seo_stat_metrics.py');
SET default_parallel 100;
SET job.name 'seo';
SET job.priority HIGH;

-- load 基础数据
LOG = LOAD '$HIVE_WAREHOUSE/visitlogs/dt=$date/*' USING org.apache.pig.piggybank.storage.HiveColumnarLoader('hour int,minute int,second int,class_name string,method_name string,mem_use int,run_time float,user_id int,agent string,refer string,type int,is_search_engine int,uri string,visitip string,poststr string,sessid string,channel_from int,tag string,http_code string,refer_class_name string,refer_method_name string,dt string,vhour string');
SPAM =  LOAD '$HIVE_WAREHOUSE/spam_daily/dt=$date' USING PigStorage('\u0001') AS (type:int, op:int, value:chararray, algo:chararray);
CMM = LOAD '$HIVE_WAREHOUSE/class_method_map/*' USING PigStorage('\u0001') AS (class_name:chararray, method_name:chararray, category:chararray);


VALID_SID = DISTINCT (FOREACH (FILTER SPAM BY type==0 and op==1) GENERATE value AS sessid); --sessid
VALID_LOG = FOREACH (JOIN LOG BY sessid, VALID_SID BY sessid) GENERATE refer, uri, visitip, LOG::sessid, class_name, method_name; --6
VALID_CMM_LOG = FOREACH (JOIN VALID_LOG BY (class_name, method_name) LEFT OUTER, CMM BY (class_name, method_name)) GENERATE refer, uri, visitip, sessid, ((CMM::class_name is null) ? 0 : 1) AS is_valid:int; -- 5

LOG_TINTY = FOREACH (FILTER VALID_CMM_LOG BY SIZE(sessid)==32) GENERATE refer,uri,visitip,sessid,is_valid;--5

-- 加上搜索引擎标识
SEARCH_ENGINE_LOG = STREAM LOG_TINTY THROUGH match_search_engine AS (search_engine:chararray, uri:chararray, sessid:chararray, visitip:chararray, is_valid:int); -- 5

SESSID_JOIN_LOG = FOREACH (JOIN SEARCH_ENGINE_LOG BY sessid, LOG_TINTY BY sessid) GENERATE search_engine, refer, SEARCH_ENGINE_LOG::uri, SEARCH_ENGINE_LOG::sessid, SEARCH_ENGINE_LOG::visitip, SEARCH_ENGINE_LOG::is_valid; -- 6

SESSID_GRP_LOG = GROUP SESSID_JOIN_LOG BY sessid;
SESSID_GRP_SORTED = FOREACH SESSID_GRP_LOG {
    GENERATE FLATTEN(SESSID_JOIN_LOG);
}

FINAL_SEARCH_ENGINE_LOG = STREAM SESSID_GRP_SORTED THROUGH seo_compute AS (search_engine:chararray, sessid:chararray, is_follow:int); --3

SEARCH_ENGINE_GRP = GROUP FINAL_SEARCH_ENGINE_LOG BY search_engine;
SEARCH_ENGINE_GRP_SORTED = FOREACH SEARCH_ENGINE_GRP {
    GENERATE FLATTEN(FINAL_SEARCH_ENGINE_LOG);
}
-- 统计结果
STAT_METRICS = STREAM SEARCH_ENGINE_GRP_SORTED THROUGH seo_stat_metrics AS (search_engine:chararray, uv:int, follwed_uv:int);
--DESCRIBE STAT_METRICS;
STORE STAT_METRICS INTO '$hdfs_store_name';
--DUMP STAT_METRICS;

