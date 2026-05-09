-- v0.5 Day 20 — Kafka Engine 主线回滚（顺序：MV → kafka 引擎表）
-- 主表 click_events_ch 不动（属 0001）
DROP VIEW IF EXISTS slink_analytics.click_events_ch_main_mv;
DROP TABLE IF EXISTS slink_analytics.click_events_ch_kafka_main;
