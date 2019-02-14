-- Table: public."DB_CAMP_Contactbase_numbers"

-- DROP TABLE public."DB_CAMP_Contactbase_numbers";

CREATE TABLE public."DB_CAMP_Contactbase_numbers"
(
    "ExternalUserID" character varying(255) COLLATE pg_catalog."default",
    "CampaignId" integer,
    "Status" boolean,
    "TenantId" integer,
    "CompanyId" integer,
    "BatchNo" character varying(255) COLLATE pg_catalog."default",
    "PreviewData" character varying(255) COLLATE pg_catalog."default",
    "CamContactBaseNumberId" integer NOT NULL DEFAULT nextval('"DB_CAMP_Contactbase_numbers_CamContactBaseNumberId_seq"'::regclass),
    "DialerStatus" "enum_DB_CAMP_Contactbase_numbers_DialerStatus",
    "createdAt" timestamp with time zone NOT NULL,
    "updatedAt" timestamp with time zone NOT NULL,
    CONSTRAINT "DB_CAMP_Contactbase_numbers_pkey" PRIMARY KEY ("CamContactBaseNumberId"),
    CONSTRAINT "DB_CAMP_Contactbase_numbers_CampaignId_fkey" FOREIGN KEY ("CampaignId")
        REFERENCES public."DB_CAMP_CampaignInfos" ("CampaignId") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE SET NULL
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public."DB_CAMP_Contactbase_numbers"
    OWNER to duo;

