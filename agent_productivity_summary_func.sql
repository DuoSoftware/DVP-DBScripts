--Author: Marlon Abeykoon
--Version: 1.0.0v

create or replace function agent_productivity_summary(from_date timestamp with time zone,
                                                      to_date timestamp with time zone, resourceid character varying,
                                                      tz character varying,
                                                      b_unit character varying, companyid integer, tenantid integer)
    returns TABLE
            (
                summary_date                        date,
                tenant                              int,
                company                             int,
                agent                               int,
                login_max_time                      int,
                login_total_count                   bigint,
                login_total_time                    text,
                login_time                          timestamptz,
                inbound_max_time                    int,
                inbound_total_count                 bigint,
                inbound_total_time                  text,
                outbound_max_time                   int,
                outbound_total_count                bigint,
                outbound_total_time                 text,
                total_call_count                    bigint,
                total_call_time                     bigint,
                avg_inbound_call_count              numeric,
                avg_outbound_call_count             numeric,
                avg_inbound_call_time               numeric,
                avg_outbound_call_time              numeric,
                inbound_talk_max_time               int,
                inbound_connected_total_count       bigint,
                inbound_talk_total_time             text,
                outbound_talk_max_time              int,
                outbound_connected_total_count      bigint,
                outbound_talk_total_time            text,
                connected_total_count               bigint,
                total_talk_total_time               bigint,
                avg_inbound_talk_time               text,
                avg_outbound_talk_time              text,
                avg_inbound_handling_time           text,
                avg_outbound_handling_time           text,
                inbound_hold_max_time               int,
                inbound_hold_total_count            bigint,
                inbound_hold_total_time             text,
                outbound_hold_max_time              int,
                outbound_hold_total_count           bigint,
                outbound_hold_total_time            text,
                total_hold_total_count              bigint,
                total_hold_total_time               bigint,
                avg_inbound_hold_count              numeric,
                avg_outbound_hold_count             numeric,
                avg_inbound_hold_time               text,
                avg_outbound_hold_time              text,
                inbound_acw_max_time                int,
                inbound_acw_total_count             bigint,
                inbound_acw_total_time              text,
                outbound_acw_max_time               int,
                outbound_acw_total_count            bigint,
                outbound_acw_total_time             text,
                total_acw_total_count               bigint,
                total_acw_total_time                bigint,
                avg_inbound_acw_count               numeric,
                avg_outbound_acw_count              numeric,
                avg_inbound_acw_time                numeric,
                avg_outbound_acw_time               numeric,
                idle_time_inbound                   text,
                idle_time_outbound                  text,
                idle_time_offline                   text,
                total_break_time                    text,
                full_total_login_time               text,
                full_total_inbound_time             text,
                full_total_outbound_time            text,
                full_total_inbound_idle_time        text,
                full_total_outbound_idle_time       text,
                full_total_offline_idle_time        text,
                full_total_inbound_acw_time         text,
                full_total_outbound_acw_time        text,
                full_total_inbound_talk_time        text,
                full_total_outbound_talk_time       text,
                full_total_inbound_hold_time        text,
                full_total_outbound_hold_time       text,
                full_total_inbound_hold_count       bigint,
                full_total_outbound_hold_count      bigint,
                full_total_break_time               text,
                full_total_connected_inbound_calls  bigint,
                full_total_connected_outbound_calls bigint,
                full_total_inbound_calls            bigint,
                full_total_outbound_calls           bigint,
                full_avg_inbound_handling_time      text,
                full_avg_outbound_handling_time     text,
                full_avg_inbound_talk_time          text,
                full_avg_outbound_talk_time         text,
                full_avg_inbound_hold_time          text,
                full_avg_outbound_hold_time         text
            )
    language plpgsql
as
$$
DECLARE

    SQL                   TEXT;
    AgentFilterExpression VARCHAR;
    BuFilterExpression    VARCHAR;

begin
    IF resourceid is null then
        AgentFilterExpression := ' ';
    ELSE
        AgentFilterExpression := ' AND "Param1" = ''' || resourceid || '''';
    end if;

    IF b_unit is null or b_unit = 'ALL' then
        BuFilterExpression := ' ';
    ELSE
        BuFilterExpression := ' AND "BusinessUnit" = ''' || b_unit || '''';

    end if;

    SQL := 'SELECT login."SummaryDate"                                                                                                        as summary_date,
       login."Tenant"::integer                                                                                                    as tenant,
       login."Company"::integer                                                                                                   as company,
       login.agent::integer                                                                                                       as agent,
       COALESCE(login.max_time, 0)                                                                                                as login_max_time,
       COALESCE(login.total_count, 0)                                                                                             as login_total_count,
       TO_CHAR(
               (
                   COALESCE(login.total_time, 0) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as login_total_time,
       COALESCE(resource_login_same_day.login_time, (select "createdAt" as login_time
                                                     from "DB_RES_ResourceStatusChangeInfos"
                                                     where "Reason" = ''Register''
                    and "CompanyId" = ' || companyid || '
                    and "TenantId" = ' || tenantid || '
                    and "createdAt" < ''' || from_date || '''
                    and "ResourceId" = login.agent::integer ' ||
           BuFilterExpression || '
                order by "createdAt" desc limit 1))                                                                               as login_time,

       COALESCE(inbound.max_time, 0)                                                                                              as inbound_max_time,
       COALESCE(inbound.total_count, 0)                                                                                           as inbound_total_count,
       TO_CHAR(
               (
                   COALESCE(inbound.total_time, 0) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as inbound_total_time,
       COALESCE(outbound.max_time, 0)                                                                                             as outbound_max_time,
       COALESCE(outbound.total_count, 0)                                                                                          as outbound_total_count,
       TO_CHAR(
               (
                   COALESCE(outbound.total_time, 0) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as outbound_total_time,
       COALESCE(inbound.total_count, 0) +
       COALESCE(outbound.total_count, 0)                                                                                          as total_call_count,
       COALESCE(inbound.total_time, 0) + COALESCE(outbound.total_time, 0)                                                         as total_call_time,
       COALESCE(inbound.avg_count, 0)                                                                                             as avg_inbound_call_count,
       COALESCE(outbound.avg_count, 0)                                                                                            as avg_outbound_call_count,
       COALESCE(inbound.avg_time, 0)                                                                                              as avg_inbound_call_time,
       COALESCE(outbound.avg_time, 0)                                                                                             as avg_outbound_call_time,

       COALESCE(inbound_connected.max_time, 0) -
       COALESCE(inbound_hold.max_time, 0)                                                                                         as inbound_talk_max_time,
       COALESCE(inbound_connected.total_count, 0)                                                                                 as inbound_connected_total_count, -- deduct inbound hold time from inbound talk time to get correct inbound talk time
       TO_CHAR(
               (
                           COALESCE(inbound_connected.total_time, 0) -
                           COALESCE(inbound_hold.total_time, 0) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as inbound_talk_total_time,
       COALESCE(outbound_connected.max_time, 0) -
       COALESCE(outbound_hold.max_time, 0)                                                                                        as outbound_talk_max_time,
       COALESCE(outbound_connected.total_count, 0)                                                                                as outbound_connected_total_count,
       TO_CHAR(
               (
                           COALESCE(outbound_connected.total_time, 0) -
                           COALESCE(outbound_hold.total_time, 0) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as outbound_talk_total_time,
       COALESCE(inbound_connected.total_count, 0) +
       COALESCE(outbound_connected.total_count, 0)                                                                                as connected_total_count,         -- deduct inbound hold time from inbound talk time to get correct inbound talk time
       COALESCE(inbound_connected.total_time, 0) - COALESCE(inbound_hold.total_time, 0) +
       COALESCE(outbound_connected.total_time, 0) -
       COALESCE(outbound_hold.total_time, 0)                                                                                      as total_talk_total_time,
       TO_CHAR(
               (COALESCE(inbound_connected.total_time, 0) /
		COALESCE(inbound_connected.total_count, 1) || '' second'' )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as avg_inbound_talk_time,
       TO_CHAR(
               (COALESCE(outbound_connected.total_time, 0) /
		COALESCE(outbound_connected.total_count, 1) || '' second'' )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as avg_outbound_talk_time,

       TO_CHAR(
               (
                           (COALESCE(inbound_connected.total_time, 0) + COALESCE(inbound_acw.total_time, 0)) /
                           COALESCE(inbound_connected.total_count, 1) || '' second '' )::interval,
				''HH24:MI:SS''
			)                               as avg_inbound_handling_time,
       TO_CHAR(
               (
                           (COALESCE(outbound_connected.total_time, 0) + COALESCE(outbound_acw.total_time, 0)) /
                           COALESCE(outbound_connected.total_count, 1) || '' second '' )::interval,
				''HH24:MI:SS''
			)                              as avg_outbound_handling_time,
       COALESCE(inbound_hold.max_time, 0)                                                                                         as inbound_hold_max_time,
       COALESCE(inbound_hold.total_count, 0)                                                                                      as inbound_total_time,
       TO_CHAR(
               (
                   COALESCE(inbound_hold.total_time, 0) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as inbound_hold_total_time,
       COALESCE(outbound_hold.max_time, 0)                                                                                        as outbound_hold_max_time,
       COALESCE(outbound_hold.total_count, 0)                                                                                     as outbound_hold_total_count,
       TO_CHAR(
               (
                   COALESCE(outbound_hold.total_time, 0) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as outbound_hold_total_time,
       COALESCE(inbound_hold.total_count, 0) +
       COALESCE(outbound_hold.total_count, 0)                                                                                     as total_hold_total_count,
       COALESCE(inbound_hold.total_time, 0) +
       COALESCE(inbound_hold.total_time, 0)                                                                                       as total_hold_total_time,
       COALESCE(inbound_hold.avg_count, 0)                                                                                        as avg_inbound_hold_count,
       COALESCE(outbound_hold.avg_count, 0)                                                                                       as avg_outbound_hold_count,
       TO_CHAR(
               (
                   COALESCE(inbound_hold.avg_time, 0) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as avg_inbound_hold_time,
       TO_CHAR(
               (
                   COALESCE(outbound_hold.avg_time, 0) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as avg_outbound_hold_time,

       COALESCE(inbound_acw.max_time, 0)                                                                                          as inbound_acw_max_time,
       COALESCE(inbound_acw.total_count, 0)                                                                                       as inbound_acw_total_count,
       TO_CHAR(
               (
                   COALESCE(inbound_acw.total_time, 0) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as inbound_acw_total_time,
       COALESCE(outbound_acw.max_time, 0)                                                                                         as outbound_acw_max_time,
       COALESCE(outbound_acw.total_count, 0)                                                                                      as outbound_acw_total_count,
       TO_CHAR(
               (
                   COALESCE(outbound_acw.total_time, 0) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as outbound_acw_total_time,
       COALESCE(inbound_acw.total_count, 0) +
       COALESCE(outbound_acw.total_count, 0)                                                                                      as total_acw_total_count,
       COALESCE(inbound_acw.total_time, 0) +
       COALESCE(outbound_acw.total_time, 0)                                                                                       as total_acw_total_time,
       COALESCE(inbound_acw.avg_count, 0)                                                                                         as avg_inbound_acw_count,
       COALESCE(outbound_acw.avg_count, 0)                                                                                        as avg_outbound_acw_count,
       COALESCE(inbound_acw.avg_time, 0)                                                                                          as avg_inbound_acw_time,
       COALESCE(outbound_acw.avg_time, 0)                                                                                         as avg_outbound_acw_time,

       TO_CHAR(
               (
                           COALESCE(inbound.total_time, 0) -
                           (COALESCE(inbound_acw.total_time, 0) +
                            COALESCE(inbound_connected.total_time, 0)) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as idle_time_inbound,
       TO_CHAR(
               (
                           COALESCE(outbound.total_time, 0) -
                           (COALESCE(outbound_acw.total_time, 0) +
                            COALESCE(outbound_connected.total_time, 0)) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as idle_time_outbound,

       TO_CHAR(
               (
                           COALESCE(login.total_time, 0) -
                           (COALESCE(inbound.total_time, 0) -
                            (COALESCE(inbound_acw.total_time, 0) + COALESCE(inbound_connected.total_time, 0)) +
                            COALESCE(outbound.total_time, 0) -
                            (COALESCE(outbound_acw.total_time, 0) +
                             COALESCE(outbound_connected.total_time, 0))) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as idle_time_offline,

       TO_CHAR(
               (
                   COALESCE(break.total_time, 0) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as total_break_time,

       TO_CHAR(
               (
                   CAST(SUM(COALESCE(login.total_time, 0)) over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as full_total_login_time,
       TO_CHAR(
               (
                   CAST(SUM(COALESCE(inbound.total_time, 0)) over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as full_total_inbound_time,
       TO_CHAR(
               (
                   CAST(SUM(COALESCE(outbound.total_time, 0)) over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as full_total_outbound_time,
       TO_CHAR(
               (
                       CAST(SUM(COALESCE(inbound.total_time, 0) -
                                (COALESCE(inbound_acw.total_time, 0) + COALESCE(inbound_connected.total_time, 0)))
                            over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as full_total_inbound_idle_time,
       TO_CHAR(
               (
                       CAST(SUM(COALESCE(outbound.total_time, 0) -
                                (COALESCE(outbound_acw.total_time, 0) + COALESCE(outbound_connected.total_time, 0)))
                            over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as full_total_outbound_idle_time,
       TO_CHAR(
               (
                       CAST(SUM(COALESCE(login.total_time, 0) -
                                (COALESCE(inbound.total_time, 0) -
                                 (COALESCE(inbound_acw.total_time, 0) + COALESCE(inbound_connected.total_time, 0)) +
                                 COALESCE(outbound.total_time, 0) -
                                 (COALESCE(outbound_acw.total_time, 0) + COALESCE(outbound_connected.total_time, 0))))
                            over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as full_total_offline_idle_time,
       TO_CHAR(
               (
                       CAST(SUM(COALESCE(inbound_acw.total_time, 0)) over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as full_total_inbound_acw_time,
       TO_CHAR(
               (
                       CAST(SUM(COALESCE(outbound_acw.total_time, 0)) over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as full_total_outbound_acw_time,
       TO_CHAR(
               (
                       CAST(SUM(COALESCE(inbound_connected.total_time, 0) - COALESCE(inbound_hold.total_time, 0))
                            over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as full_total_inbound_talk_time,
       TO_CHAR(
               (
                       CAST(SUM(COALESCE(outbound_connected.total_time, 0) - COALESCE(outbound_hold.total_time, 0))
                            over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as full_total_outbound_talk_time,
       TO_CHAR(
               (
                       CAST(SUM(COALESCE(inbound_hold.total_time, 0)) over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as full_total_inbound_hold_time,
       TO_CHAR(
               (
                       CAST(SUM(COALESCE(outbound_hold.total_time, 0)) over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as full_total_outbound_hold_time,
       CAST(
               SUM(COALESCE(inbound_hold.total_count, 0) ) over () AS BIGINT)                                                     as full_total_inbound_hold_count,
       CAST(
               SUM(COALESCE(outbound_hold.total_count, 0) ) over () AS BIGINT)                                                     as full_total_outbound_hold_count,
       TO_CHAR(
               (
                   CAST(SUM(COALESCE(break.total_time, 0)) over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )                                                                                                                      as full_total_break_time,
       CAST(
               SUM(COALESCE(inbound_connected.total_count, 0)) over () AS BIGINT)                                                 as full_total_connected_inbound_calls,
       CAST(
               SUM(COALESCE(outbound_connected.total_count, 0)) over () AS BIGINT)                                                as full_total_connected_outbound_calls,
       CAST(SUM(COALESCE(inbound.total_count, 0)) over () AS BIGINT)                                                              as full_total_inbound_calls,
       CAST(SUM(COALESCE(outbound.total_count, 0)) over () AS BIGINT)                                                             as full_total_outbound_calls,
       TO_CHAR(
               (
                           (CAST(SUM(COALESCE(inbound_connected.total_time, 0)) over () AS BIGINT) +
                            CAST(SUM(COALESCE(inbound_acw.total_time, 0)) over () AS BIGINT)) /
                           CAST(SUM(COALESCE(inbound_connected.total_count, 1)) over () AS BIGINT) || '' second '' )::interval,
				''HH24:MI:SS''
			)  as full_avg_inbound_handling_time,
       TO_CHAR(
               (
                           (CAST(SUM(COALESCE(outbound_connected.total_time, 0)) over () AS BIGINT) +
                            CAST(SUM(COALESCE(outbound_acw.total_time, 0)) over () AS BIGINT)) /
                           CAST(SUM(COALESCE(outbound_connected.total_count, 1)) over () AS BIGINT) || '' second '' )::interval,
				''HH24:MI:SS''
			) as full_avg_outbound_handling_time,
       TO_CHAR(
               (
                       CAST(AVG(COALESCE(inbound_connected.total_time, 0) - COALESCE(inbound_connected.total_time, 0))
                            over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )  as full_avg_inbound_talk_time,
       TO_CHAR(
               (
                       CAST(AVG(COALESCE(outbound_connected.total_time, 0) - COALESCE(outbound_connected.total_time, 0))
                            over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )  as full_avg_outbound_talk_time,
       TO_CHAR(
               (
                       CAST(AVG(COALESCE(inbound_hold.total_time, 0)) over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )   as full_avg_inbound_hold_time,
       TO_CHAR(
               (
                       CAST(AVG(COALESCE(outbound_hold.total_time, 0)) over () AS BIGINT) || '' second''
                   )::interval,
               '' HH24:MI:SS ''
           )   as full_avg_outbound_hold_time
FROM ((select "SummaryDate"::date,
              "Tenant",
              "Company",
              "Param1"          as agent,
              max("MaxTime")    as max_time,
              sum("TotalCount") as total_count,
              sum("TotalTime")  as total_time,
              avg("TotalCount") as avg_count,
              avg("TotalTime")  as avg_time

       from "Dashboard_DailySummaries"
       where "WindowName" = ''LOGIN''
           and "Company" = ''' || companyid || '''
           and "Tenant" = ''' || tenantid || '''
           and "SummaryDate" >= ''' || from_date || '''
           and "SummaryDate" <= ''' || to_date || ''' '
               || AgentFilterExpression || ' '
               || BuFilterExpression ||
           '
       group by "SummaryDate"::date, "Tenant", "Company", "Param1") as login
         left outer join
     (select "createdAt"::date,
             "TenantId",
             "CompanyId",
             "ResourceId"     as agent,
             min("createdAt") as login_time
      from "DB_RES_ResourceStatusChangeInfos"
      where "Reason" = ''Register''
          and "CompanyId" = ' || companyid || '
          and "TenantId" = ' || tenantid || '
      group by "createdAt"::date, "TenantId", "CompanyId", "ResourceId"
      order by login_time
     ) as resource_login_same_day
     on login."SummaryDate"::date = resource_login_same_day."createdAt"::date and
        login."Tenant"::integer = resource_login_same_day."TenantId" and
        login."Company"::integer = resource_login_same_day."CompanyId" and
        login.agent::integer = resource_login_same_day.agent


         left outer join
     (select "SummaryDate"::date,
             "Tenant",
             "Company",
             "Param1"          as agent,
             max("MaxTime")    as max_time,
             sum("TotalCount") as total_count,
             sum("TotalTime")  as total_time,
             avg("TotalCount") as avg_count,
             avg("TotalTime")  as avg_time
      from "Dashboard_DailySummaries"
      where "WindowName" = ''INBOUND''
          and "Company" = ''' || companyid || '''
          and "Tenant" = ''' || tenantid || '''
                and "SummaryDate" >= ''' || from_date || '''
           and "SummaryDate" <= ''' || to_date || ''' '
               || AgentFilterExpression || ' '
               || BuFilterExpression ||
           '
           group by "SummaryDate"::date, "Tenant", "Company", "Param1") as inbound
     on login."SummaryDate" = inbound."SummaryDate" and login."Tenant" = inbound."Tenant" and
        login."Company" = inbound."Company" and login.agent = inbound.agent
         left outer join
     (select "SummaryDate"::date,
             "Tenant",
             "Company",
             "Param1"          as agent,
             max("MaxTime")    as max_time,
             sum("TotalCount") as total_count,
             sum("TotalTime")  as total_time,
             avg("TotalCount") as avg_count,
             avg("TotalTime")  as avg_time
      from "Dashboard_DailySummaries"
      where "WindowName" = ''OUTBOUND''
          and "Company" = ''' || companyid || '''
          and "Tenant" = ''' || tenantid || '''
                 and "SummaryDate" >= ''' || from_date || '''
           and "SummaryDate" <= ''' || to_date || ''' '
               || AgentFilterExpression || ' '
               || BuFilterExpression ||
           '
           group by "SummaryDate"::date, "Tenant", "Company", "Param1") as outbound
     on login."SummaryDate" = outbound."SummaryDate" and login."Tenant" = outbound."Tenant" and
        login."Company" = outbound."Company" and login.agent = outbound.agent
         left outer join
     (select "SummaryDate"::date,
             "Tenant",
             "Company",
             "Param1"          as agent,
             max("MaxTime")    as max_time,
             sum("TotalCount") as total_count,
             sum("TotalTime")  as total_time,
             avg("TotalCount") as avg_count,
             avg("TotalTime")  as avg_time
      from "Dashboard_DailySummaries"
      where "WindowName" = ''CONNECTED''
          and "Param2" = ''CALLinbound''
          and "Company" = ''' || companyid || '''
          and "Tenant" = ''' || tenantid || '''
                 and "SummaryDate" >= ''' || from_date || '''
           and "SummaryDate" <= ''' || to_date || ''' '
               || AgentFilterExpression || ' '
               || BuFilterExpression ||
           '
           group by "SummaryDate"::date, "Tenant", "Company", "Param1") as inbound_connected
     on login."SummaryDate" = inbound_connected."SummaryDate" and login."Tenant" = inbound_connected."Tenant" and
        login."Company" = inbound_connected."Company" and login.agent = inbound_connected.agent

         left outer join
     (select "SummaryDate"::date,
             "Tenant",
             "Company",
             "Param1"          as agent,
             max("MaxTime")    as max_time,
             sum("TotalCount") as total_count,
             sum("TotalTime")  as total_time,
             avg("TotalCount") as avg_count,
             avg("TotalTime")  as avg_time
      from "Dashboard_DailySummaries"
      where "WindowName" = ''CONNECTED''
          and "Param2" = ''CALLoutbound''
          and "Company" = ''' || companyid || '''
          and "Tenant" = ''' || tenantid || '''
                and "SummaryDate" >= ''' || from_date || '''
           and "SummaryDate" <= ''' || to_date || ''' '
               || AgentFilterExpression || ' '
               || BuFilterExpression ||
           '
      group by "SummaryDate"::date, "Tenant", "Company", "Param1") as outbound_connected
     on login."SummaryDate" = outbound_connected."SummaryDate" and login."Tenant" = outbound_connected."Tenant" and
        login."Company" = outbound_connected."Company" and login.agent = outbound_connected.agent

         left outer join
     (select "SummaryDate"::date,
             "Tenant",
             "Company",
             "Param1"          as agent,
             max("MaxTime")    as max_time,
             sum("TotalCount") as total_count,
             sum("TotalTime")  as total_time,
             avg("TotalCount") as avg_count,
             avg("TotalTime")  as avg_time
      from "Dashboard_DailySummaries"
      where "WindowName" = ''AGENTHOLD''
          and "Param2" = ''inbound''
          and "Company" = ''' || companyid || '''
          and "Tenant" = ''' || tenantid || '''
                 and "SummaryDate" >= ''' || from_date || '''
           and "SummaryDate" <= ''' || to_date || ''' '
               || AgentFilterExpression || ' '
               || BuFilterExpression ||
           '
           group by "SummaryDate"::date, "Tenant", "Company", "Param1") as inbound_hold
     on login."SummaryDate" = inbound_hold."SummaryDate" and login."Tenant" = inbound_hold."Tenant" and
        login."Company" = inbound_hold."Company" and login.agent = inbound_hold.agent
         left outer join
     (select "SummaryDate"::date,
             "Tenant",
             "Company",
             "Param1"          as agent,
             max("MaxTime")    as max_time,
             sum("TotalCount") as total_count,
             sum("TotalTime")  as total_time,
             avg("TotalCount") as avg_count,
             avg("TotalTime")  as avg_time
      from "Dashboard_DailySummaries"
      where "WindowName" = ''AGENTHOLD''
          and "Param2" = ''outbound''
          and "Company" = ''' || companyid || '''
          and "Tenant" = ''' || tenantid || '''
                 and "SummaryDate" >= ''' || from_date || '''
           and "SummaryDate" <= ''' || to_date || ''' '
               || AgentFilterExpression || ' '
               || BuFilterExpression ||
           '
           group by "SummaryDate"::date, "Tenant", "Company", "Param1") as outbound_hold
     on login."SummaryDate" = outbound_hold."SummaryDate" and login."Tenant" = outbound_hold."Tenant" and
        login."Company" = outbound_hold."Company" and login.agent = outbound_hold.agent
         left outer join
     (select "SummaryDate"::date,
             "Tenant",
             "Company",
             "Param1"          as agent,
             max("MaxTime")    as max_time,
             sum("TotalCount") as total_count,
             sum("TotalTime")  as total_time,
             avg("TotalCount") as avg_count,
             avg("TotalTime")  as avg_time
      from "Dashboard_DailySummaries"
      where "WindowName" = ''AFTERWORK''
          and "Param2" = ''AfterWorkCALLinbound''
          and "Company" = ''' || companyid || '''
          and "Tenant" = ''' || tenantid || '''
                and "SummaryDate" >= ''' || from_date || '''
           and "SummaryDate" <= ''' || to_date || ''' '
               || AgentFilterExpression || ' '
               || BuFilterExpression ||
           '
           group by "SummaryDate"::date, "Tenant", "Company", "Param1") as inbound_acw
     on login."SummaryDate" = inbound_acw."SummaryDate" and login."Tenant" = inbound_acw."Tenant" and
        login."Company" = inbound_acw."Company" and login.agent = inbound_acw.agent
         left outer join
     (select "SummaryDate"::date,
             "Tenant",
             "Company",
             "Param1"          as agent,
             max("MaxTime")    as max_time,
             sum("TotalCount") as total_count,
             sum("TotalTime")  as total_time,
             avg("TotalCount") as avg_count,
             avg("TotalTime")  as avg_time
      from "Dashboard_DailySummaries"
      where "WindowName" = ''AFTERWORK''
          and "Param2" = ''AfterWorkCALLoutbound''
          and "Company" = ''' || companyid || '''
          and "Tenant" = ''' || tenantid || '''
                 and "SummaryDate" >= ''' || from_date || '''
           and "SummaryDate" <= ''' || to_date || ''' '
               || AgentFilterExpression || ' '
               || BuFilterExpression ||
           '
           group by "SummaryDate"::date, "Tenant", "Company", "Param1") as outbound_acw
     on login."SummaryDate" = outbound_acw."SummaryDate" and login."Tenant" = outbound_acw."Tenant" and
        login."Company" = outbound_acw."Company" and login.agent = outbound_acw.agent
         left outer join
     (select "SummaryDate"::date,
             "Tenant",
             "Company",
             "Param1"          as agent,
             max("MaxTime")    as max_time,
             sum("TotalCount") as total_count,
             sum("TotalTime")  as total_time,
             avg("TotalCount") as avg_count,
             avg("TotalTime")  as avg_time
      from "Dashboard_DailySummaries"
      where "WindowName" = ''BREAK''
          and "Company" = ''' || companyid || '''
          and "Tenant" = ''' || tenantid || '''
                 and "SummaryDate" >= ''' || from_date || '''
           and "SummaryDate" <= ''' || to_date || ''' '
               || AgentFilterExpression || ' '
               || BuFilterExpression ||
           '
           group by "SummaryDate"::date, "Tenant", "Company", "Param1") as break
     on login."SummaryDate" = break."SummaryDate" and login."Tenant" = break."Tenant" and
        login."Company" = break."Company" and login.agent = break.agent
         )

order by summary_date, tenant, company, agent;';
    raise notice 'SQL: %', SQL;
    RETURN QUERY EXECUTE SQL;
EXCEPTION
    WHEN OTHERS THEN
        RAISE;
end
$$;
