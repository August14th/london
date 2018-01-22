-- 创建数据库和用户
CREATE DATABASE london;
CREATE USER london WITH PASSWORD 'london';
GRANT ALL PRIVILEGES ON DATABASE london to london;
-- 建表
CREATE TABLE subscribes (
  "topicid" varchar(255) COLLATE "pg_catalog"."default" NOT NULL DEFAULT NULL,
  "userid" varchar(255) COLLATE "pg_catalog"."default" NOT NULL DEFAULT NULL,
  "lastackid" int8 NOT NULL DEFAULT NULL,
  CONSTRAINT "subscribes_pkey" PRIMARY KEY ("topicid", "userid")
);
CREATE TABLE topics (
  "id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL DEFAULT NULL,
  "lastmsgid" int8 NOT NULL DEFAULT NULL,
  CONSTRAINT "topics_pkey" PRIMARY KEY ("id")
);