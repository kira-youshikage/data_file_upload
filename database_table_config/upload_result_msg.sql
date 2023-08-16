CREATE TABLE "kira"."upload_result_msg" (
  "table_name" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "file_name" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "file_size" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "file_time" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "upload_time" timestamp(6) DEFAULT now(),
  CONSTRAINT "upload_result_msg_pkey" PRIMARY KEY ("file_time", "file_size", "file_name", "table_name")
)
;