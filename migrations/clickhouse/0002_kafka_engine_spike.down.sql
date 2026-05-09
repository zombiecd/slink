-- v0.5 Day 18 — 第三组 spike 回滚（顺序：MV → target → kafka 引擎表）
DROP VIEW IF EXISTS slink_analytics.click_events_ch_kafka_mv;
DROP TABLE IF EXISTS slink_analytics.click_events_ch_kafka_target;
DROP TABLE IF EXISTS slink_analytics.click_events_ch_kafka_spike;
