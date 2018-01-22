CREATE TABLE "public"."subscribes" (
  "topicid" varchar(255) COLLATE "pg_catalog"."default" NOT NULL DEFAULT NULL,
  "userid" varchar(255) COLLATE "pg_catalog"."default" NOT NULL DEFAULT NULL,
  "lastackid" int8 NOT NULL DEFAULT NULL,
  CONSTRAINT "subscribes_pkey" PRIMARY KEY ("topicid", "userid")
)
;

CREATE TABLE "public"."topics" (
  "id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL DEFAULT NULL,
  "lastmsgid" int8 NOT NULL DEFAULT NULL,
  CONSTRAINT "topics_pkey" PRIMARY KEY ("id")
)
;

CREATE ROLE london;

ALTER TABLE "public"."topics"
  OWNER TO "london";

ALTER TABLE "public"."subscribes"
  OWNER TO "london";