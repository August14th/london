CREATE TABLE "public"."subscribes" (
  "topicid" varchar(255) COLLATE "pg_catalog"."default" NOT NULL DEFAULT NULL,
  "userid" varchar(255) COLLATE "pg_catalog"."default" NOT NULL DEFAULT NULL,
  "lastackid" int8 NOT NULL DEFAULT NULL,
  CONSTRAINT "subscribes_pkey" PRIMARY KEY ("topicid", "userid")
)
;

ALTER TABLE "public"."subscribes"
  OWNER TO "london";