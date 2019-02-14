CREATE TABLE public."DB_RES_Attribute_User_Groups"
(
    id SERIAL NOT NULL,
    "GroupId" character varying(255) COLLATE pg_catalog."default",
    "GroupName" character varying(255) COLLATE pg_catalog."default",
    "AttributeId" integer,
    "AttributeGroupId" integer,
    "CompanyId" integer,
    "TenantId" integer,
    "createdAt" timestamp with time zone NOT NULL,
    "updatedAt" timestamp with time zone NOT NULL,
    CONSTRAINT "DB_RES_Attribute_User_Groups_pkey" PRIMARY KEY (id),
    CONSTRAINT "DB_RES_Attribute_User_Groups_GroupId_AttributeId_key" UNIQUE ("GroupId", "AttributeId"),
    CONSTRAINT "DB_RES_Attribute_User_Groups_AttributeGroupId_fkey" FOREIGN KEY ("AttributeGroupId")
        REFERENCES public."DB_RES_AttributeGroups" ("AttributeGroupId") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT "DB_RES_Attribute_User_Groups_AttributeId_fkey" FOREIGN KEY ("AttributeId")
        REFERENCES public."DB_RES_Attributes" ("AttributeId") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
 
ALTER TABLE public."DB_RES_Attribute_User_Groups"
    OWNER to duo;

