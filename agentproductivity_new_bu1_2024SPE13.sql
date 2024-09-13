-- FUNCTION: public.agentproductivity_new_bu1(timestamp with time zone, timestamp with time zone)

-- DROP FUNCTION IF EXISTS public.agentproductivity_new_bu1(timestamp with time zone, timestamp with time zone);

CREATE OR REPLACE FUNCTION public.agentproductivity_new_bu1(
	from_date timestamp with time zone,
	to_date timestamp with time zone)
    RETURNS TABLE(summary_date date, tenant integer, company integer, bu character varying, agent integer, loginn_time timestamp with time zone, login_total_time text, inbound_connected_total_count bigint, outbound_connected_total_count bigint, connected_total_count bigint, outbound_dialed_total_count bigint, outbound_dialed_total_time text, avg_outbound_dialed_time text, inbound_talk_total_time text, avg_inbound_talk_time text, inbound_hold_total_time text, avg_inbound_hold_time text, outbound_talk_total_time text, avg_outbound_talk_time text, outbound_hold_total_time text, avg_outbound_hold_time text, inbound_acw_total_time text, outbound_acw_total_time text, avg_inbound_handling_time character varying, avg_outbound_handling_time character varying, total_break_time text, inbound_total_time text, inbound_total_count bigint, outbound_total_time text, outbound_total_count bigint, idle_time_inbound text, idle_time_outbound text, idle_time_offline text, logout_time timestamp with time zone) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    SQL TEXT;
    converted_from_date timestamp with time zone;
    converted_to_date timestamp with time zone;
    input_time_zone TEXT;
BEGIN
    -- Convert the provided date to the local time zone
    input_time_zone := '+05:30';
    converted_from_date := from_date AT TIME ZONE 'UTC';
    converted_to_date := to_date AT TIME ZONE 'UTC';

    -- Dynamic SQL query construction using format()
    SQL := format(
        'WITH acw_times AS (
            SELECT 
                "ResourceId" AS agent,
                SUM(CASE WHEN "CSDB_CallCDRProcesseds"."DVPCallDirection" = ''inbound'' THEN "DB_RES_ResourceAcwInfos"."Duration" ELSE 0 END) AS inbound_total_acw_time,
                SUM(CASE WHEN "CSDB_CallCDRProcesseds"."DVPCallDirection" = ''outbound'' THEN "DB_RES_ResourceAcwInfos"."Duration" ELSE 0 END) AS outbound_total_acw_time
            FROM 
                "DB_RES_ResourceAcwInfos"
            INNER JOIN 
                "CSDB_CallCDRProcesseds" 
                ON "DB_RES_ResourceAcwInfos"."SessionId" = "CSDB_CallCDRProcesseds"."Uuid"
            WHERE 
                "DB_RES_ResourceAcwInfos"."createdAt" >= %L
                AND "DB_RES_ResourceAcwInfos"."createdAt" <= %L
            GROUP BY "ResourceId"
        ),
        break_times AS (
            SELECT 
                "ResourceId",
                DATE_TRUNC(''day'', "createdAt") AS "SummaryDate",
                TO_CHAR(
                    INTERVAL ''1 second'' * SUM("Duration"),
                    ''HH24:MI:SS''
                ) AS "FormattedBreakTime"
            FROM 
                "DB_RES_ResourceStatusDurationInfos"
            WHERE "StatusType" = ''ResourceStatus''
                AND "Status" = ''NotAvailable''
                AND "createdAt" BETWEEN %L AND %L
            GROUP BY "SummaryDate","ResourceId"
        ),
        RankedEvents AS (
            SELECT
                "ResourceId",
                "createdAt",
                "Reason",
                "TenantId",
                "CompanyId",
                "BusinessUnit",
                ROW_NUMBER() OVER (PARTITION BY "ResourceId" ORDER BY "createdAt") AS rn,
                LEAD("createdAt") OVER (PARTITION BY "ResourceId" ORDER BY "createdAt") AS NextCreatedAt,
                LEAD("Reason") OVER (PARTITION BY "ResourceId" ORDER BY "createdAt") AS NextReason
            FROM public."DB_RES_ResourceStatusChangeInfos"
            WHERE "Reason" IN (''Register'', ''UnRegister'')
              AND "createdAt" >= %L
              AND "createdAt" <= %L
        ),
        Durations AS (
            SELECT 
                r1."ResourceId",
                r1."TenantId",
                r1."CompanyId",
                 r1."BusinessUnit",
                (r1.NextCreatedAt - r1."createdAt") AS Duration
            FROM RankedEvents r1
            WHERE r1."Reason" = ''Register''
              AND r1.NextReason = ''UnRegister''
        ),
        TotalDurations AS (
            SELECT 
                "ResourceId",
                "TenantId",
                "CompanyId",
                "BusinessUnit",
                SUM(Duration) AS TotalDuration
            FROM Durations
            GROUP BY "TenantId","CompanyId","BusinessUnit","ResourceId"
        ),
          RankedEvents_Inbound AS (
            SELECT
                "ResourceId",
                "createdAt",
                "Reason",
                "TenantId",
                "CompanyId",
                "BusinessUnit",
                ROW_NUMBER() OVER (PARTITION BY "ResourceId" ORDER BY "createdAt") AS rn,
                LEAD("createdAt") OVER (PARTITION BY "ResourceId" ORDER BY "createdAt") AS NextCreatedAt,
                LEAD("Reason") OVER (PARTITION BY "ResourceId" ORDER BY "createdAt") AS NextReason,
                SUM(CASE WHEN "Reason" = ''endInbound''THEN 1 ELSE 0 END) OVER (PARTITION BY "ResourceId" ORDER BY "createdAt" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS EndInboundCount
            FROM public."DB_RES_ResourceStatusChangeInfos"
            WHERE "Reason" IN (''Inbound'', ''endInbound'')
            AND "createdAt" >= %L
            AND "createdAt" <= %L
        ),
        Filtered_Inbound AS (
            SELECT
                r1."ResourceId",
                r1."TenantId",
                r1."CompanyId",
                r1."BusinessUnit",
                r1."createdAt" AS InboundTime,
                r1.NextCreatedAt AS EndInboundTime,
                (r1.NextCreatedAt - r1."createdAt") AS Duration
            FROM RankedEvents_Inbound r1
            WHERE r1."Reason" = ''Inbound''
              AND (
                    r1.EndInboundCount = 
                    (
                        SELECT MIN(r2.EndInboundCount) 
                        FROM RankedEvents_Inbound r2 
                        WHERE r2."ResourceId" = r1."ResourceId" 
                        AND r2.rn >= r1.rn 
                        AND r2."Reason" = ''Inbound''
                    )
                )
           ),
         TotalDurationsInbounds AS (
            SELECT
                "ResourceId",
                "TenantId",
                "CompanyId",
                "BusinessUnit",
                SUM(Duration) AS TotalDurationInbound
            FROM Filtered_Inbound
            GROUP BY  "TenantId", "CompanyId", "BusinessUnit","ResourceId"
        ),
        RankedEvents_Outbound AS (
            SELECT
                "ResourceId",
                "createdAt",
                "Reason",
                "TenantId",
                "CompanyId",
                "BusinessUnit",
                ROW_NUMBER() OVER (PARTITION BY "ResourceId" ORDER BY "createdAt") AS rn,
                LEAD("createdAt") OVER (PARTITION BY "ResourceId" ORDER BY "createdAt") AS NextCreatedAt,
                LEAD("Reason") OVER (PARTITION BY "ResourceId" ORDER BY "createdAt") AS NextReason,
                SUM(CASE WHEN "Reason" = ''endOutbound''THEN 1 ELSE 0 END) OVER (PARTITION BY "ResourceId" ORDER BY "createdAt" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS EndOutboundCount
            FROM public."DB_RES_ResourceStatusChangeInfos"
            WHERE "Reason" IN (''Outbound'', ''endOutbound'')
            AND "createdAt" >= %L
            AND "createdAt" <= %L
        ),
        Filtered_Outbound AS (
            SELECT
                r1."ResourceId",
                r1."TenantId",
                r1."CompanyId",
                r1."BusinessUnit",
                r1."createdAt" AS OutboundTime,
                r1.NextCreatedAt AS EndOutboundTime,
                (r1.NextCreatedAt - r1."createdAt") AS Duration
            FROM RankedEvents_Outbound r1
            WHERE r1."Reason" = ''Outbound''
              AND (
                    r1.EndOutboundCount = 
                    (
                        SELECT MIN(r2.EndOutboundCount) 
                        FROM RankedEvents_Outbound r2 
                        WHERE r2."ResourceId" = r1."ResourceId" 
                        AND r2.rn >= r1.rn 
                        AND r2."Reason" = ''Outbound''
                    )
                )
           ),
        TotalDurationsOutbounds AS (
            SELECT
                "ResourceId",
                "TenantId",
                "CompanyId",
                "BusinessUnit",
                SUM(Duration) AS TotalDurationOutbound
            FROM Filtered_Outbound
            GROUP BY "TenantId", "CompanyId", "BusinessUnit","ResourceId"
        )
        SELECT 
            main_query."createdAt" AS summary_date,  -- Cast to date
            main_query."TenantId" AS tenant,
            main_query."CompanyId" AS company,
            main_query."BusinessUnit" AS bu,
            main_query."agent" AS agent,
            COALESCE(main_query."LoginTime", (
                SELECT "createdAt" AS login_time
                FROM "DB_RES_ResourceStatusChangeInfos"
                WHERE "Reason" = ''Register''
                  AND "createdAt" < %L
                  AND "ResourceId" = main_query.agent::integer
                ORDER BY "createdAt" DESC LIMIT 1
            )) AS loginn_time,
           COALESCE(TO_CHAR(
                COALESCE(td.TotalDuration, INTERVAL ''0 seconds''),
                ''HH24:MI:SS''
            ),
            ''00:00:00''
           ) AS login_total_time,
           COALESCE(inbound.Inbound_Connected_TotalCount::bigint, 0) AS inbound_connected_totalcount,
           COALESCE(outbound.Outbound_Connected_TotalCount::bigint, 0) AS outbound_connected_total_count,
           COALESCE(inbound.Inbound_Connected_TotalCount::bigint, 0)+COALESCE(outbound.Outbound_Connected_TotalCount::bigint, 0) AS  connected_total_count,
           COALESCE(outbound.Outbound_Dialed_TotalCount::bigint, 0) AS outbound_dialed_total_count,
           TO_CHAR(
            (COALESCE(outbound.Outbound_Dialed_TotalTime::bigint, 0) * INTERVAL ''1 second''),
            '' HH24:MI:SS ''
            )AS outbound_dialed_total_time,
           TO_CHAR(
            CASE 
                WHEN COALESCE(outbound.Outbound_Dialed_TotalCount, 1) > 0 
                THEN (
                    COALESCE(outbound.Outbound_Dialed_TotalTime::bigint, 0) / 
                    COALESCE(outbound.Outbound_Dialed_TotalCount, 1))* INTERVAL ''1 second''
                ELSE ''00:00:00''::interval
            END,
            ''HH24:MI:SS''
            )AS avg_outbound_dialed_time,
           TO_CHAR(
               (COALESCE(inbound.Inbound_Connected_TotalTime::bigint, 0) -
                 COALESCE(inbound.Hold_Time_Inbound::bigint, 0))* INTERVAL ''1 second'',
           '' HH24:MI:SS '')AS inbound_talk_total_time,
           TO_CHAR(
            CASE 
                WHEN COALESCE(inbound.Inbound_Connected_TotalCount, 1) > 0 
                     AND COALESCE(inbound.Inbound_Connected_TotalTime, 0) >= COALESCE(inbound.Hold_Time_Inbound, 0)
                THEN ((COALESCE(inbound.Inbound_Connected_TotalTime::bigint, 0) - COALESCE(inbound.Hold_Time_Inbound::bigint, 0)) / 
                      COALESCE(inbound.Inbound_Connected_TotalCount, 1))* INTERVAL ''1 second''
                ELSE ''00:00:00''::interval 
            END,
            '' HH24:MI:SS ''
           )AS avg_inbound_talk_time,
           TO_CHAR(
            (COALESCE(inbound.Hold_Time_Inbound::bigint, 0) * INTERVAL ''1 second''),
            '' HH24:MI:SS ''
            )AS inbound_hold_total_time,
           TO_CHAR(
            CASE 
                WHEN COALESCE(inbound.Inbound_Hold_Count, 1) > 0 
                THEN (COALESCE(inbound.Hold_Time_Inbound::bigint, 0) / 
                      COALESCE(inbound.Inbound_Hold_Count, 1))* INTERVAL ''1 second''
                ELSE ''00:00:00''::interval 
            END,
            '' HH24:MI:SS ''
           )AS avg_inbound_hold_time,
           TO_CHAR(
               (COALESCE(outbound.Outbound_Connected_TotalTime::bigint, 0) -
                 COALESCE(outbound.Hold_TalkTime_Outbound::bigint, 0))* INTERVAL ''1 second'',
           '' HH24:MI:SS '') AS outbound_talk_total_time,
           TO_CHAR(
            CASE 
                WHEN COALESCE(outbound.Outbound_Connected_TotalCount, 1) > 0 
                     AND COALESCE(outbound.Outbound_Connected_TotalTime, 0) >= COALESCE(outbound.Hold_TalkTime_Outbound, 0)
                THEN ((COALESCE(outbound.Outbound_Connected_TotalTime::bigint, 0) - COALESCE(outbound.Hold_TalkTime_Outbound::bigint, 0)) / 
                      COALESCE(outbound.Outbound_Connected_TotalCount, 1))* INTERVAL ''1 second''
                ELSE ''00:00:00''::interval 
            END,
            '' HH24:MI:SS ''
           )AS avg_outbound_talk_time,
           TO_CHAR(
            (COALESCE(outbound.Hold_TalkTime_Outbound::bigint, 0) * INTERVAL ''1 second''),
            '' HH24:MI:SS ''
            )AS outbound_hold_total_time,
           TO_CHAR(
            CASE 
                WHEN COALESCE(outbound.Outbound_Hold_Count, 1) > 0 
                THEN (COALESCE(outbound.Hold_TalkTime_Outbound::bigint, 0) / 
                      COALESCE(outbound.Outbound_Hold_Count, 1))* INTERVAL ''1 second''
                ELSE ''00:00:00''::interval 
            END,
            '' HH24:MI:SS ''
           )AS avg_outbound_hold_time,
           TO_CHAR(
            (COALESCE(acw.inbound_total_acw_time::bigint, 0) * INTERVAL ''1 second''),
            ''HH24:MI:SS'') AS inbound_acw_total_time,
           TO_CHAR(
            (COALESCE(acw.outbound_total_acw_time::bigint, 0) * INTERVAL ''1 second''),
            ''HH24:MI:SS'') AS outbound_acw_total_time,
           TO_CHAR(
            CASE 
                WHEN COALESCE(inbound.Inbound_Connected_TotalCount, 1) > 0 
                THEN ((COALESCE(inbound.Inbound_Connected_TotalTime::bigint, 0) + COALESCE(acw.inbound_total_acw_time::bigint, 0)) / 
                      COALESCE(inbound.Inbound_Connected_TotalCount, 1))* INTERVAL ''1 second''
                ELSE ''00:00:00''::interval 
            END,
            ''HH24:MI:SS''
           )::character varying AS avg_inbound_handling_time,
           TO_CHAR(
            CASE 
                WHEN COALESCE(outbound.Outbound_Connected_TotalCount, 1) > 0 
                THEN ((COALESCE(outbound.Outbound_Connected_TotalTime::bigint, 0) + COALESCE(acw.outbound_total_acw_time::bigint, 0)) / 
                      COALESCE(outbound.Outbound_Connected_TotalCount, 1))* INTERVAL ''1 second''
                ELSE ''00:00:00''::interval 
            END,
            ''HH24:MI:SS''
           )::character varying AS avg_outbound_handling_time,
           COALESCE(break_times."FormattedBreakTime", ''00:00:00'') AS total_break_time,
           COALESCE(TO_CHAR(
                COALESCE(ti.TotalDurationInbound, INTERVAL ''0 seconds''),
                ''HH24:MI:SS''
            ),
            ''00:00:00''
           ) AS inbound_total_time,
           COALESCE(inbound.Total_Inbound, 0) AS inbound_total_count,
           COALESCE(TO_CHAR(
                COALESCE(tout.TotalDurationOutbound, INTERVAL ''0 seconds''),
                ''HH24:MI:SS''
            ),
            ''00:00:00''
           ) AS outbound_total_time,
           COALESCE(outbound.Total_Outbound, 0) AS outbound_total_count,
           TO_CHAR(
                (
                    COALESCE(ti.TotalDurationInbound, INTERVAL ''0 seconds'') - 
                    (
                        (COALESCE(acw.inbound_total_acw_time, 0) * INTERVAL ''1 second'') +
                        (COALESCE(inbound.Inbound_Connected_TotalTime, 0) * INTERVAL ''1 second'')
                    )
                )::interval,
                ''HH24:MI:SS''
            ) AS idle_time_inbound,
            TO_CHAR(
                (
                    COALESCE(tout.TotalDurationOutbound, INTERVAL ''0 seconds'') - 
                    (
                        (COALESCE(acw.outbound_total_acw_time, 0) * INTERVAL ''1 second'') +
                        (COALESCE(outbound.Outbound_Connected_TotalTime, 0) * INTERVAL ''1 second'')
                    )
                )::interval,
                ''HH24:MI:SS''
            ) AS idle_time_outbound,
            TO_CHAR(
                (   
                    COALESCE(td.TotalDuration, INTERVAL ''0 seconds'')-
                    (COALESCE(ti.TotalDurationInbound, INTERVAL ''0 seconds'') +
                    COALESCE(tout.TotalDurationOutbound, INTERVAL ''0 seconds''))+
                    ((COALESCE(acw.inbound_total_acw_time, 0) * INTERVAL ''1 second'') +
                     (COALESCE(inbound.Inbound_Connected_TotalTime, 0) * INTERVAL ''1 second''))+
                    ((COALESCE(acw.outbound_total_acw_time, 0) * INTERVAL ''1 second'') +
                    (COALESCE(outbound.Outbound_Connected_TotalTime, 0) * INTERVAL ''1 second''))
                )::interval,
                ''HH24:MI:SS''
            ) AS idle_time_offline,
           COALESCE(main_query."LogoutTime", CONCAT(
                TO_CHAR(main_query."createdAt", ''yyyy-mm-dd''), '' 18:29:59.00+00''
            )::timestamp with time zone) AS logout_time
        FROM (
            SELECT 
                "createdAt"::date,
                "TenantId",
                "CompanyId",
                "BusinessUnit",
                "ResourceId" AS "agent",
                MIN(CASE WHEN "Reason" = ''Register'' THEN "createdAt" ELSE NULL END) AS "LoginTime",
                MAX(CASE WHEN "Reason" = ''UnRegister'' THEN "createdAt" ELSE NULL END) AS "LogoutTime"
            FROM "DB_RES_ResourceStatusChangeInfos"
            WHERE "createdAt" >= %L
              AND "createdAt" <= %L
            GROUP BY 
                "createdAt"::date, 
                "TenantId", 
                "CompanyId", 
                "BusinessUnit", 
                "ResourceId"
        ) AS main_query
        LEFT OUTER JOIN (
                 SELECT 
                    ("CreatedTime" AT TIME ZONE ''UTC'' AT TIME ZONE %L)::date AS "LocalTime",
                    cdr."TenantId",
                    cdr."CompanyId",
                    cdr."BusinessUnit",
                    res."ResourceId" AS "ResourceId",
                    COUNT(*) FILTER (WHERE cdr."DVPCallDirection" = ''inbound'') AS Total_Inbound,
                    COUNT(*) FILTER (WHERE cdr."DVPCallDirection" = ''inbound'' AND cdr."AgentAnswered"=true) AS Inbound_Connected_TotalCount,
                    SUM(CASE WHEN cdr."DVPCallDirection" = ''inbound'' AND cdr."AgentAnswered"=true THEN cdr."BillSec" ELSE 0 END) AS Inbound_Connected_TotalTime,
                    AVG(CASE WHEN cdr."DVPCallDirection" = ''inbound'' AND cdr."AgentAnswered"=true THEN cdr."BillSec" END) AS Average_Inbound_Connected,
                    SUM(CASE WHEN cdr."DVPCallDirection" = ''inbound'' AND cdr."AgentAnswered" THEN cdr."HoldSec" ELSE 0 END) AS Hold_Time_Inbound,
                    COUNT(*) FILTER (WHERE cdr."DVPCallDirection" = ''inbound'' AND cdr."AgentAnswered" = true AND cdr."HoldSec" > 0) AS Inbound_Hold_Count
                FROM 
                    "CSDB_CallCDRProcesseds" AS cdr
                LEFT JOIN 
                    "DB_RES_Resources" res

                ON 
                    cdr."RecievedBy" = res."ResourceName"

                WHERE 
                    cdr."CreatedTime" >=  %L
                    AND cdr."CreatedTime" <= %L
                    AND res."ResourceId" IS NOT NULL
                GROUP BY  
                    ("CreatedTime" AT TIME ZONE ''UTC'' AT TIME ZONE %L)::date, 
                    cdr."TenantId", 
                    cdr."CompanyId", 
                    cdr."BusinessUnit",
                    res."ResourceId"
        ) AS inbound
        ON 
        main_query."createdAt"= inbound."LocalTime" 
        AND main_query."TenantId" = inbound."TenantId"
        AND main_query."CompanyId" = inbound."CompanyId"
        AND main_query."BusinessUnit" = inbound."BusinessUnit"
        AND main_query."agent" = inbound."ResourceId"
        LEFT OUTER JOIN (
        SELECT 
                ("CreatedTime" AT TIME ZONE ''UTC'' AT TIME ZONE %L)::date AS "LocalTime",
                cdr."TenantId",
                cdr."CompanyId",
                cdr."BusinessUnit",
                res."ResourceId" AS "ResourceId",
                COUNT(*) FILTER (WHERE cdr."DVPCallDirection" = ''outbound'') AS Total_Outbound,
                COUNT(*) FILTER (WHERE cdr."DVPCallDirection" = ''outbound'' AND cdr."IsAnswered" = true) AS Outbound_Connected_TotalCount,
                COUNT(*) FILTER (WHERE cdr."DVPCallDirection" = ''outbound'' AND cdr."IsAnswered" = false) AS Outbound_Dialed_TotalCount,

                SUM(CASE WHEN cdr."DVPCallDirection" = ''outbound'' AND cdr."IsAnswered" = true THEN cdr."BillSec" ELSE 0 END) AS Outbound_Connected_TotalTime,
                AVG(CASE WHEN cdr."DVPCallDirection" = ''outbound'' AND cdr."IsAnswered" = true THEN cdr."BillSec" END) AS Average_Outbound_Connected,

                SUM(CASE WHEN cdr."DVPCallDirection" = ''outbound'' AND cdr."IsAnswered" THEN cdr."HoldSec" ELSE 0 END) AS Hold_TalkTime_Outbound,
                COUNT(*) FILTER (WHERE cdr."DVPCallDirection" = ''outbound'' AND "IsAnswered"=true AND "HoldSec" > 0) AS Outbound_Hold_Count,
                SUM(CASE WHEN cdr."DVPCallDirection" = ''outbound'' AND cdr."IsAnswered" = false AND cdr."ObjType" != ''PRIVATE_USER'' THEN cdr."IvrConnectSec" ELSE 0 END) AS Outbound_Dialed_TotalTime,
                AVG( CASE WHEN cdr."DVPCallDirection" = ''outbound'' AND cdr."IsAnswered" = false AND cdr."ObjType" != ''PRIVATE_USER'' THEN cdr."IvrConnectSec" ELSE 0 END) AS Average_Outbound_DialedTime

            FROM 
              "CSDB_CallCDRProcesseds" cdr
            LEFT JOIN 
              "DB_RES_Resources" res
            ON 
              cdr."SipFromUser" = res."ResourceName" 

            WHERE 
                cdr."CreatedTime" >=  %L
                AND cdr."CreatedTime" <=  %L
                AND res."ResourceId" IS NOT NULL
            GROUP BY  
                ("CreatedTime" AT TIME ZONE ''UTC'' AT TIME ZONE %L)::date, 
                cdr."TenantId", 
                cdr."CompanyId",
                cdr."BusinessUnit",
                res."ResourceId"
                
        )AS outbound
        ON 
        main_query."createdAt"= outbound."LocalTime" 
        AND main_query."TenantId" = outbound."TenantId"
        AND main_query."CompanyId" = outbound."CompanyId"
        AND main_query."BusinessUnit" = outbound."BusinessUnit"
        AND main_query."agent" = outbound."ResourceId"
        LEFT JOIN acw_times acw
        ON acw.agent = main_query.agent::integer
        LEFT JOIN break_times 
        ON break_times."ResourceId" = main_query.agent::integer
        AND break_times."SummaryDate" = main_query."createdAt"
        LEFT JOIN TotalDurations td
        ON main_query.agent = td."ResourceId"::integer
        LEFT JOIN TotalDurationsInbounds ti
        ON main_query.agent = ti."ResourceId"::integer
        LEFT JOIN TotalDurationsOutbounds tout
        ON main_query.agent = tout."ResourceId"::integer
   ORDER BY main_query."createdAt";',
        converted_from_date,
        converted_to_date,
        converted_from_date,
        converted_to_date,
        converted_from_date,
        converted_to_date,
        converted_from_date,
        converted_to_date,
        converted_from_date,
        converted_to_date,
        converted_from_date, -- Parameter for the COALESCE login time subquery
        converted_from_date,
        converted_to_date,
        input_time_zone,
        converted_from_date,
        converted_to_date,
        input_time_zone,
        input_time_zone,
        converted_from_date,
        converted_to_date,
        input_time_zone
    );

    RAISE NOTICE 'SQL: %', SQL;
    RETURN QUERY EXECUTE SQL;
EXCEPTION
    WHEN OTHERS THEN
        RAISE;
END
$BODY$;

ALTER FUNCTION public.agentproductivity_new_bu1(timestamp with time zone, timestamp with time zone)
    OWNER TO duo;
