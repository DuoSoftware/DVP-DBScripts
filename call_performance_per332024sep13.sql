-- FUNCTION: public.call_performance_per33(timestamp with time zone, timestamp with time zone)

-- DROP FUNCTION IF EXISTS public.call_performance_per33(timestamp with time zone, timestamp with time zone);

CREATE OR REPLACE FUNCTION public.call_performance_per33(
	from_date timestamp with time zone,
	to_date timestamp with time zone)
    RETURNS TABLE(id integer, date date, tenant integer, company integer, bu character varying, total_inbound bigint, total_outbound bigint, total_queued bigint, total_queue_dropped bigint, total_queue_answered bigint, total_outbound_answered bigint, total_talktime_inbound bigint, avg_talktime_inbound bigint, holdtime_inbound bigint, total_talktime_outbound bigint, avg_talktime_outbound bigint, holdtime_outbound bigint, total_staff_count bigint, total_staff_time bigint, average_staff_time bigint, total_acw_time bigint, average_acw_time bigint, total_break_time bigint, average_inbound_call_per_agent numeric, average_outbound_call_per_agent numeric, total_idle_time bigint) 
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
    input_time_zone := '+05:30';
    converted_from_date := from_date AT TIME ZONE 'UTC';
    converted_to_date := to_date AT TIME ZONE 'UTC';
    -- Use format() to safely include parameters
    SQL := format(
        'SELECT 
            CAST(row_number() OVER() AS integer) AS id,
            main."LocalTime"::date AS date,  -- Cast to date
            main."TenantId" AS tenant,
            main."CompanyId" AS company,
            main."BusinessUnit" AS bu,
            COALESCE(main.Total_Inbound, 0) AS total_inbound,
            COALESCE(main.Total_Outbound, 0) AS total_outbound,
            COALESCE(main.Total_Queued, 0) AS total_queued,
            COALESCE(main.Total_Queue_Dropped, 0) AS total_queue_dropped,
            COALESCE(main.Total_Queue_Answered, 0) AS total_queue_answered,
            COALESCE(main.Total_Outbound_Answered, 0) AS total_outbound_answered,
            COALESCE(main.Total_TalkTime_Inbound::bigint, 0) AS total_talktime_inbound,
            COALESCE(main.Average_TalkTime_Inbound::bigint, 0) AS avg_talktime_inbound,
            COALESCE(main.Hold_Time_Inbound::bigint, 0) AS holdtime_inbound,
            COALESCE(main.Total_TalkTime_Outbound::bigint, 0) AS total_talktime_outbound,
            COALESCE(main.Average_TalkTime_Outbound::bigint, 0) AS avg_talktime_outbound,
            COALESCE(main.Hold_TalkTime_Outbound::bigint, 0) AS holdtime_outbound,
            COALESCE(daily.TotalStaffCount, 0) AS total_staff_count,
            COALESCE(daily.TotalStaffTime::bigint, 0) AS total_staff_time,
            CASE
                WHEN COALESCE(daily.TotalStaffCount, 0) > 0 THEN
                    COALESCE(daily.TotalStaffTime::bigint, 0) / COALESCE(daily.TotalStaffCount, 0)
                ELSE
                    0
            END AS average_staff_time,
            COALESCE(daily.TotalAcwTime::bigint, 0) AS total_acw_time,
            CASE
                WHEN COALESCE(daily.TotalStaffCount, 0) > 0 THEN
                    COALESCE(daily.TotalAcwTime::bigint, 0) / COALESCE(daily.TotalStaffCount, 0)
                ELSE
                    0
            END AS average_acw_time,
            COALESCE(daily.TotalBreakTime::bigint, 0) AS total_break_time,
            CASE 
                WHEN COALESCE(main.Inbound_Agent_Count, 0) = 0 THEN NULL
                ELSE ROUND(
                    COALESCE(main.Inbound_Answer_Count, 0)::NUMERIC / NULLIF(main.Inbound_Agent_Count, 0)::NUMERIC, 1
                )
            END AS average_inbound_call_per_agent,
            CASE 
                WHEN COALESCE(main.Outbound_Agent_Count, 0) = 0 THEN NULL
                ELSE ROUND(
                    COALESCE(main.Outbound_Answer_Count, 0)::NUMERIC / NULLIF(main.Outbound_Agent_Count, 0)::NUMERIC, 
                    1
                )
            END AS average_outbound_call_per_agent,
            COALESCE(daily.TotalStaffTime::bigint, 0) - (
                COALESCE(daily.TotalAcwTime::bigint, 0) +
                COALESCE(main.Total_TalkTime_Inbound::bigint, 0) +
                COALESCE(main.Total_TalkTime_Outbound::bigint, 0) +
                COALESCE(daily.TotalBreakTime::bigint, 0) +
                COALESCE(main.Hold_Time_Inbound::bigint, 0) +
                COALESCE(main.Hold_TalkTime_Outbound::bigint, 0)
            ) AS total_idle_time
        FROM (
            SELECT 
                ("CreatedTime" AT TIME ZONE''UTC'' AT TIME ZONE%L)::date AS "LocalTime",
                "TenantId",
                "CompanyId",
                "BusinessUnit",
                COUNT(*) FILTER (WHERE "DVPCallDirection" = ''inbound'') AS Total_Inbound,
                COUNT(*) FILTER (WHERE "DVPCallDirection" = ''outbound'') AS Total_Outbound,
                COUNT(CASE WHEN "IsQueued" THEN 1 END) AS Total_Queued,
                COUNT(CASE WHEN "IsQueued" AND "AgentAnswered" = false THEN 1 END) AS Total_Queue_Dropped,
                COUNT(*) FILTER (WHERE "DVPCallDirection" = ''inbound'' AND "AgentAnswered") AS Total_Queue_Answered,
                COUNT(*) FILTER (WHERE "DVPCallDirection" = ''outbound'' AND "IsAnswered") AS Total_Outbound_Answered,
                SUM(CASE WHEN "DVPCallDirection" = ''inbound'' AND "AgentAnswered" THEN "BillSec" ELSE 0 END) AS Total_TalkTime_Inbound,
                AVG(CASE WHEN "DVPCallDirection" = ''inbound'' AND "AgentAnswered" THEN "BillSec" END) AS Average_TalkTime_Inbound,
                SUM(CASE WHEN "DVPCallDirection" = ''inbound'' AND "AgentAnswered" THEN "HoldSec" ELSE 0 END) AS Hold_Time_Inbound,
                SUM(CASE WHEN "DVPCallDirection" = ''outbound'' AND "IsAnswered" THEN "BillSec" ELSE 0 END) AS Total_TalkTime_Outbound,
                AVG(CASE WHEN "DVPCallDirection" = ''outbound'' AND "IsAnswered" THEN "BillSec" END) AS Average_TalkTime_Outbound,
                SUM(CASE WHEN "DVPCallDirection" = ''outbound'' AND "IsAnswered" THEN "HoldSec" ELSE 0 END) AS Hold_TalkTime_Outbound,
                COUNT(DISTINCT CASE  WHEN "DVPCallDirection" = ''inbound'' AND "IsAnswered" = true AND "ObjType" = ''HTTAPI'' THEN "RecievedBy" ELSE NULL END) AS Inbound_Agent_Count,
                COUNT(*) FILTER (WHERE "DVPCallDirection" = ''inbound''  AND "IsAnswered" = true AND "ObjType" = ''HTTAPI'') AS Inbound_Answer_Count,
                COUNT(*) FILTER (WHERE "DVPCallDirection" = ''outbound''  AND "IsAnswered" = true AND "ObjType" != ''PRIVATE_USER'') AS Outbound_Answer_Count,
                COUNT(DISTINCT CASE  WHEN "DVPCallDirection" = ''outbound'' AND "IsAnswered" = true AND "ObjType" != ''PRIVATE_USER'' THEN "SipFromUser" ELSE NULL END) AS Outbound_Agent_Count
            FROM "CSDB_CallCDRProcesseds"
            WHERE "CreatedTime" >= %L
              AND "CreatedTime" <= %L
            GROUP BY  ("CreatedTime" AT TIME ZONE''UTC'' AT TIME ZONE%L)::date, "TenantId", "CompanyId", "BusinessUnit"
        ) AS main
        LEFT OUTER JOIN (
            SELECT 
                "SummaryDate"::date AS Date,
                "Tenant",
                "Company",
                "BusinessUnit",
                COUNT(*) FILTER (WHERE "WindowName" = ''LOGIN'') AS TotalStaffCount,
                SUM(CASE WHEN "WindowName" = ''LOGIN'' THEN "TotalTime" ELSE 0 END) AS TotalStaffTime,
                SUM(CASE WHEN "WindowName" = ''AFTERWORK'' THEN "TotalTime" ELSE 0 END) AS TotalAcwTime,
                SUM(CASE WHEN "WindowName" = ''BREAK'' THEN "TotalTime" ELSE 0 END) AS TotalBreakTime
            FROM "Dashboard_DailySummaries"
            WHERE "SummaryDate" >= %L
              AND "SummaryDate" <= %L
            GROUP BY "SummaryDate"::date, "Tenant", "Company", "BusinessUnit"
        ) AS daily
        ON main."LocalTime" = daily.Date
        AND main."TenantId" = daily."Tenant"::integer
        AND main."CompanyId" = daily."Company"::integer
        AND main."BusinessUnit" = daily."BusinessUnit"
        ORDER BY main."LocalTime";',
        input_time_zone,
        converted_from_date,
        converted_to_date,
        input_time_zone,
        converted_from_date,
        converted_to_date
    );

    RAISE NOTICE 'SQL: %', SQL;
    RETURN QUERY EXECUTE SQL;
EXCEPTION
    WHEN OTHERS THEN
        RAISE;
END
$BODY$;

ALTER FUNCTION public.call_performance_per33(timestamp with time zone, timestamp with time zone)
    OWNER TO duo;
