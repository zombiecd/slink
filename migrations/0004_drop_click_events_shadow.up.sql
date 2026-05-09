-- ============================================================
-- slink v0.4 — Day 16 切流后清理影子表
-- 设计文档: docs/architecture/v0.4-kafka.md §8.3 切流第 4 步
--
-- Day 15 影子期已结束（端到端 0 漏验证 + Day 16 切流验证主表 OK）。
-- 这里直接 DROP 影子表 + 子分区。
--
-- 回滚预案：跑 0004_drop_click_events_shadow.down.sql 重建（schema 同 0003）。
-- ============================================================

DROP TABLE IF EXISTS click_events_shadow_2026_06;
DROP TABLE IF EXISTS click_events_shadow_2026_05;
DROP TABLE IF EXISTS click_events_shadow;
