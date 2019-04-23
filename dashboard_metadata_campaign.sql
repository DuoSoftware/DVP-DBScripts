/* Dashboard Meta Data - Campaign Dashboards */

INSERT INTO public."Dashboard_MetaData" ("EventClass", "EventType", "EventCategory", "WindowName", "Count", "FlushEnable", "UseSession", "PersistSession", "ThresholdEnable", "ThresholdValue", "DashboardMetaDataId", "createdAt", "updatedAt") VALUES ('DIALER', 'CAMPAIGN', 'DIALING', 'CAMPAIGNDIALING', 1, true, true, false, false, 0, 57, '2017-08-23 05:18:47.433-04', '2017-08-23 05:18:47.433-04');

INSERT INTO public."Dashboard_MetaData" ("EventClass", "EventType", "EventCategory", "WindowName", "Count", "FlushEnable", "UseSession", "PersistSession", "ThresholdEnable", "ThresholdValue", "DashboardMetaDataId", "createdAt", "updatedAt") VALUES ('DIALER', 'CAMPAIGN', 'DISCONNECTING', 'CAMPAIGNDIALING', -1, true, true, false, false, 0, 58, '2017-08-23 05:18:47.433-04', '2017-08-23 05:18:47.433-04');

INSERT INTO public."Dashboard_MetaData" ("EventClass", "EventType", "EventCategory", "WindowName", "Count", "FlushEnable", "UseSession", "PersistSession", "ThresholdEnable", "ThresholdValue", "DashboardMetaDataId", "createdAt", "updatedAt") VALUES ('DIALER', 'CAMPAIGN', 'DISCONNECTED', 'CAMPAIGNCONNECTED', -1, true, true, false, false, 0, 61, '2017-08-23 05:18:47.433-04', '2017-08-23 05:18:47.433-04');

INSERT INTO public."Dashboard_MetaData" ("EventClass", "EventType", "EventCategory", "WindowName", "Count", "FlushEnable", "UseSession", "PersistSession", "ThresholdEnable", "ThresholdValue", "DashboardMetaDataId", "createdAt", "updatedAt") VALUES ('DIALER', 'CAMPAIGN', 'CONNECTED', 'CAMPAIGNCONNECTED', 1, true, true, false, false, 0, 60, '2017-08-23 05:18:47.433-04', '2017-08-23 05:18:47.433-04');

INSERT INTO public."Dashboard_MetaData" ("EventClass", "EventType", "EventCategory", "WindowName", "Count", "FlushEnable", "UseSession", "PersistSession", "ThresholdEnable", "ThresholdValue", "DashboardMetaDataId", "createdAt", "updatedAt") VALUES ('DIALER', 'CAMPAIGN', 'NUMBERADDED', 'CAMPAIGNNUMBERSTAKEN', 1, true, true, false, false, 0, 62, '2017-08-23 05:18:47.433-04', '2017-08-23 05:18:47.433-04');

INSERT INTO public."Dashboard_MetaData" ("EventClass", "EventType", "EventCategory", "WindowName", "Count", "FlushEnable", "UseSession", "PersistSession", "ThresholdEnable", "ThresholdValue", "DashboardMetaDataId", "createdAt", "updatedAt") VALUES ('DIALER', 'CAMPAIGN', 'REJECTED', 'CAMPAIGNREJECTED', 1, true, false, false, false, 0, 63, '2019-03-25 00:00:00-04', '2019-03-25 00:00:00-04');


/* Dashboard Publish Meta Data - Campaign Dashboards */

INSERT INTO public."Dashboard_Publish_MetaData" ("WindowName", "EventName", "DashboardPubMetaDataId", "createdAt", "updatedAt") VALUES ('CAMPAIGNCONNECTED', 'TotalCount', 47, '2018-12-10 00:00:00-05', '2018-12-10 00:00:00-05');

INSERT INTO public."Dashboard_Publish_MetaData" ("WindowName", "EventName", "DashboardPubMetaDataId", "createdAt", "updatedAt") VALUES ('CAMPAIGNDIALING', 'TotalCount', 48, '2018-12-10 00:00:00-05', '2018-12-10 00:00:00-05');

INSERT INTO public."Dashboard_Publish_MetaData" ("WindowName", "EventName", "DashboardPubMetaDataId", "createdAt", "updatedAt") VALUES ('CAMPAIGNNUMBERSTAKEN', 'TotalCount', 51, '2018-12-10 00:00:00-05', '2018-12-10 00:00:00-05');

INSERT INTO public."Dashboard_Publish_MetaData" ("WindowName", "EventName", "DashboardPubMetaDataId", "createdAt", "updatedAt") VALUES ('CAMPAIGNCONNECTED', 'CurrentCount', 52, '2018-12-10 00:00:00-05', '2018-12-10 00:00:00-05');

INSERT INTO public."Dashboard_Publish_MetaData" ("WindowName", "EventName", "DashboardPubMetaDataId", "createdAt", "updatedAt") VALUES ('CAMPAIGNDIALING', 'CurrentCount', 53, '2018-12-10 00:00:00-05', '2018-12-10 00:00:00-05');

INSERT INTO public."Dashboard_Publish_MetaData" ("WindowName", "EventName", "DashboardPubMetaDataId", "createdAt", "updatedAt") VALUES ('CAMPAIGNREJECTED', 'TotalCount', 54, '2019-12-10 00:00:00-05', '2019-12-10 00:00:00-05');