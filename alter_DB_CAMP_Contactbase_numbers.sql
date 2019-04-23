ALTER TABLE public."DB_CAMP_Contactbase_numbers"
ADD COLUMN "CamScheduleId" integer;

ALTER TABLE public."DB_CAMP_Contactbase_numbers"
ADD CONSTRAINT "CampContactbaseNumbersIndex" UNIQUE ("CampaignId", "TenantId", "CompanyId", "ExternalUserID", "CamScheduleId");

