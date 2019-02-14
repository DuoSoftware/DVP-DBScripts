CREATE TABLE public."DB_RES_Attribute_Business_Units"
(
    id SERIAL NOT NULL,
    "BUId" character varying(255) COLLATE pg_catalog."default",
    "UnitName" character varying(255) COLLATE pg_catalog."default",
    "AttributeId" integer,
    "AttributeGroupId" integer,
    "TenantId" integer,
    "CompanyId" integer,
    "createdAt" timestamp with time zone NOT NULL,
    "updatedAt" timestamp with time zone NOT NULL,
    CONSTRAINT "DB_RES_Attribute_Business_Units_pkey" PRIMARY KEY (id),
    CONSTRAINT "DB_RES_Attribute_Business_Units_BUId_AttributeId_key" UNIQUE ("BUId", "AttributeId"),
    CONSTRAINT "DB_RES_Attribute_Business_Units_AttributeGroupId_fkey" FOREIGN KEY ("AttributeGroupId")
        REFERENCES public."DB_RES_AttributeGroups" ("AttributeGroupId") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT "DB_RES_Attribute_Business_Units_AttributeId_fkey" FOREIGN KEY ("AttributeId")
        REFERENCES public."DB_RES_Attributes" ("AttributeId") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
 
ALTER TABLE public."DB_RES_Attribute_Business_Units"
    OWNER to duo;

