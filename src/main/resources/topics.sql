CREATE TABLE "public"."topics" (
  "id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL DEFAULT NULL,
  "lastmsgid" int8 NOT NULL DEFAULT NULL,
  CONSTRAINT "topics_pkey" PRIMARY KEY ("id")
)
;

ALTER TABLE "public"."topics"
  OWNER TO "london";