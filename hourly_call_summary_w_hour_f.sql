--Version: 1.0.1v

-- FUNCTION: public.hourly_call_summary_w_hour_f(timestamp with time zone, timestamp with time zone, integer, integer, character varying, character varying, character varying, integer, integer)

-- DROP FUNCTION public.hourly_call_summary_w_hour_f(timestamp with time zone, timestamp with time zone, integer, integer, character varying, character varying, character varying, integer, integer);

CREATE OR REPLACE FUNCTION public.hourly_call_summary_w_hour_f(
	from_date timestamp with time zone,
	to_date timestamp with time zone,
	from_hour integer,
	to_hour integer,
	skills character varying,
	tz character varying,
	b_unit character varying,
	company integer,
	tenant integer)
    RETURNS TABLE(ivrcount bigint, queuedcount bigint, abandonedcount bigint, abandonedpercent numeric, droppedcount bigint, droppedpercent numeric, holdsec_avg text, ivrconnect_avg text, answersec_avg text, billsec_avg text, answered_count bigint, queuesec_avg text, answeredpercent numeric, abandonedqueue_avg text, answeredqueue_avg text, s_date date, s_hour integer, agentskill character varying, company_id integer, tenant_id integer)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS $BODY$
 begin if b_unit is null then return query select
			a.ivrcount,
			b.queuedcount,
			c.abandonedcount,
			(
				c.abandonedcount * 100 / b.queuedcount
			)::numeric as abandonedpercent,
			j.droppedcount,
			(
				j.droppedcount * 100 / b.queuedcount
			)::numeric as droppedpercent,
			TO_CHAR(
				(
					d.holdsec_avg || ' second'
				)::interval,
				'HH24:MI:SS'
			),
			TO_CHAR(
				(
					e.ivrconnect_avg || ' second'
				)::interval,
				'HH24:MI:SS'
			),
			TO_CHAR(
				(
					f.answersec_avg || ' second'
				)::interval,
				'HH24:MI:SS'
			),
			TO_CHAR(
				(
					g.billsec_avg || ' second'
				)::interval,
				'HH24:MI:SS'
			),
			h.answered_count,
			TO_CHAR(
				(
					i.queuesec_avg || ' second'
				)::interval,
				'HH24:MI:SS'
			),
			(
				h.answered_count * 100 / b.queuedcount
			)::numeric as answeredpercent,
			TO_CHAR(
				(
					k.abandonedqueue_avg || ' second'
				)::interval,
				'HH24:MI:SS'
			),
			TO_CHAR(
				(
					l.answeredqueue_avg || ' second'
				)::interval,
				'HH24:MI:SS'
			),
			series.s_date,
			series.s_hour,
			a."AgentSkill",
			a."CompanyId",
			a."TenantId"
		from
			(
				(
					select
						date_trunc(
							'day',
							dd
						):: date as s_date,
						date_part(
							'hour',
							dd
						)::integer as s_hour
					from
						generate_series (
							timezone(
								'Asia/Colombo',
								from_date
							) ,
							timezone(
								'Asia/Colombo',
								to_date
							) ,
							'1 hour'::interval
						) dd
						where date_part(
							'hour',
							dd
						)::integer >= from_hour
						and date_part(
							'hour',
							dd
						)::integer <= to_hour
				) as series
			left outer join (
					select
						count(*) as "ivrcount",
						timezone(
							'Asia/Colombo',
							"CSDB_CallCDRProcessed"."CreatedTime"
						)::date as c_date,
						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer as c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
					from
						"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
					where
						"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
						and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
						and "CSDB_CallCDRProcessed"."CompanyId" = company
						and "CSDB_CallCDRProcessed"."TenantId" = tenant
						and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
						and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
						and "CSDB_CallCDRProcessed"."AgentSkill" = any (
							regexp_split_to_array(
								skills,
								','
							)
						)
					group by
						c_date,
						c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
				) a on
				series.s_date = a.c_date
				and series.s_hour = a.c_hour
			left outer join (
					select
						count(*) as "queuedcount",
						timezone(
							'Asia/Colombo',
							"CSDB_CallCDRProcessed"."CreatedTime"
						)::date as c_date,
						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer as c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
					from
						"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
					where
						"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
						and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
						and "CSDB_CallCDRProcessed"."CompanyId" = company
						and "CSDB_CallCDRProcessed"."TenantId" = tenant
						and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
						and "CSDB_CallCDRProcessed"."IsQueued" = true
						and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
						and "CSDB_CallCDRProcessed"."AgentSkill" = any (
							regexp_split_to_array(
								skills,
								','
							)
						)
					group by
						c_date,
						c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
				) b on
				a."CompanyId" = b."CompanyId"
				and a."TenantId" = b."TenantId"
				and a."AgentSkill" = b."AgentSkill"
				and a.c_date = b.c_date
				and a.c_hour = b.c_hour
			left outer join (
					select
						count(*) as "abandonedcount",
						timezone(
							'Asia/Colombo',
							"CSDB_CallCDRProcessed"."CreatedTime"
						)::date as c_date,
						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer as c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
					from
						"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
					where
						"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
						and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
						and "CSDB_CallCDRProcessed"."CompanyId" = company
						and "CSDB_CallCDRProcessed"."TenantId" = tenant
						and "CSDB_CallCDRProcessed"."IsQueued" = true
						and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
						and "CSDB_CallCDRProcessed"."QueueSec" > 5
						and "CSDB_CallCDRProcessed"."AgentAnswered" = false
						and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
						and "CSDB_CallCDRProcessed"."AgentSkill" = any (
							regexp_split_to_array(
								skills,
								','
							)
						)
					group by
						c_date,
						c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
				) c on
				a."CompanyId" = c."CompanyId"
				and a."TenantId" = c."TenantId"
				and a."AgentSkill" = c."AgentSkill"
				and a.c_date = c.c_date
				and a.c_hour = c.c_hour
			left outer join (
					select
						avg( "HoldSec" ) as "holdsec_avg",
						timezone(
							'Asia/Colombo',
							"CSDB_CallCDRProcessed"."CreatedTime"
						)::date as c_date,
						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer as c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
					from
						"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
					where
						"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
												and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
						and "CSDB_CallCDRProcessed"."HoldSec" > 0
						and "CSDB_CallCDRProcessed"."CompanyId" = company
						and "CSDB_CallCDRProcessed"."TenantId" = tenant
						and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
						and "CSDB_CallCDRProcessed"."AgentAnswered" = true
						and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
						and "CSDB_CallCDRProcessed"."AgentSkill" = any (
							regexp_split_to_array(
								skills,
								','
							)
						)
					group by
						c_date,
						c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
				) d on
				a."CompanyId" = d."CompanyId"
				and a."TenantId" = d."TenantId"
				and a."AgentSkill" = d."AgentSkill"
				and a.c_date = d.c_date
				and a.c_hour = d.c_hour
			left outer join (
					select
						avg("IvrConnectSec") as "ivrconnect_avg",
						timezone(
							'Asia/Colombo',
							"CSDB_CallCDRProcessed"."CreatedTime"
						)::date as c_date,
						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer as c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
					from
						"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
					where
						"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
												and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
						and "CSDB_CallCDRProcessed"."CompanyId" = company
						and "CSDB_CallCDRProcessed"."TenantId" = tenant
						and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
						and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
						and "CSDB_CallCDRProcessed"."AgentSkill" = any (
							regexp_split_to_array(
								skills,
								','
							)
						)
					group by
						c_date,
						c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
				) e on
				a."CompanyId" = e."CompanyId"
				and a."TenantId" = e."TenantId"
				and a."AgentSkill" = e."AgentSkill"
				and a.c_date = e.c_date
				and a.c_hour = e.c_hour
			left outer join (
					select
						avg("AnswerSec") as "answersec_avg",
						timezone(
							'Asia/Colombo',
							"CSDB_CallCDRProcessed"."CreatedTime"
						)::date as c_date,
						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer as c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
					from
						"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
					where
						"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
												and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
						and "CSDB_CallCDRProcessed"."CompanyId" = company
						and "CSDB_CallCDRProcessed"."TenantId" = 1
						and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
						and "CSDB_CallCDRProcessed"."AgentAnswered" = true
						and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
						and "CSDB_CallCDRProcessed"."AgentSkill" = any (
							regexp_split_to_array(
								skills,
								','
							)
						)
					group by
						c_date,
						c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
				) f on
				a."CompanyId" = f."CompanyId"
				and a."TenantId" = f."TenantId"
				and a."AgentSkill" = f."AgentSkill"
				and a.c_date = f.c_date
				and a.c_hour = f.c_hour
			left outer join (
					select
						avg("BillSec") as "billsec_avg",
						timezone(
							'Asia/Colombo',
							"CSDB_CallCDRProcessed"."CreatedTime"
						)::date as c_date,
						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer as c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
					from
						"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
					where
						"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
												and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
						and "CSDB_CallCDRProcessed"."CompanyId" = company
						and "CSDB_CallCDRProcessed"."TenantId" = tenant
						and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
						and "CSDB_CallCDRProcessed"."AgentAnswered" = true
						and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
						and "CSDB_CallCDRProcessed"."AgentSkill" = any (
							regexp_split_to_array(
								skills,
								','
							)
						)
					group by
						c_date,
						c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
				) g on
				a."CompanyId" = g."CompanyId"
				and a."TenantId" = g."TenantId"
				and a."AgentSkill" = g."AgentSkill"
				and a.c_date = g.c_date
				and a.c_hour = g.c_hour
			left outer join (
					select
						count(*) as "answered_count",
						timezone(
							'Asia/Colombo',
							"CSDB_CallCDRProcessed"."CreatedTime"
						)::date as c_date,
						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer as c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
					from
						"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
					where
						"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
												and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
						and "CSDB_CallCDRProcessed"."CompanyId" = company
						and "CSDB_CallCDRProcessed"."TenantId" = tenant
						and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
						and "CSDB_CallCDRProcessed"."AgentAnswered" = true
						and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
						and "CSDB_CallCDRProcessed"."AgentSkill" = any (
							regexp_split_to_array(
								skills,
								','
							)
						)
					group by
						c_date,
						c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
				) h on
				a."CompanyId" = h."CompanyId"
				and a."TenantId" = h."TenantId"
				and a."AgentSkill" = h."AgentSkill"
				and a.c_date = h.c_date
				and a.c_hour = h.c_hour
			left outer join (
					select
						avg("QueueSec") as "queuesec_avg",
						timezone(
							'Asia/Colombo',
							"CSDB_CallCDRProcessed"."CreatedTime"
						)::date as c_date,
						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer as c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
					from
						"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
					where
						"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
												and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
						and "CSDB_CallCDRProcessed"."CompanyId" = company
						and "CSDB_CallCDRProcessed"."TenantId" = tenant
						and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
						and "CSDB_CallCDRProcessed"."IsQueued" = true
						and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
						and "CSDB_CallCDRProcessed"."AgentSkill" = any (
							regexp_split_to_array(
								skills,
								','
							)
						)
					group by
						c_date,
						c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
				) i on
				a."CompanyId" = i."CompanyId"
				and a."TenantId" = i."TenantId"
				and a."AgentSkill" = i."AgentSkill"
				and a.c_date = i.c_date
				and a.c_hour = i.c_hour
			left outer join(
					select
						count(*) as "droppedcount",
						timezone(
							'Asia/Colombo',
							"CSDB_CallCDRProcessed"."CreatedTime"
						)::date as c_date,
						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer as c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
					from
						"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
					where
						"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
												and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
						and "CSDB_CallCDRProcessed"."CompanyId" = company
						and "CSDB_CallCDRProcessed"."TenantId" = tenant
						and "CSDB_CallCDRProcessed"."IsQueued" = true
						and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
						and "CSDB_CallCDRProcessed"."QueueSec" <= 5
						and "CSDB_CallCDRProcessed"."AgentAnswered" = false
						and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
						and "CSDB_CallCDRProcessed"."AgentSkill" = any (
							regexp_split_to_array(
								skills,
								','
							)
						)
					group by
						c_date,
						c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
				) j on
				a."CompanyId" = j."CompanyId"
				and a."TenantId" = j."TenantId"
				and a."AgentSkill" = j."AgentSkill"
				and a.c_date = j.c_date
				and a.c_hour = j.c_hour
			left outer join (
					select
						avg("QueueSec") as "abandonedqueue_avg",
						timezone(
							'Asia/Colombo',
							"CSDB_CallCDRProcessed"."CreatedTime"
						)::date as c_date,
						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer as c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
					from
						"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
					where
						"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
												and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
						and "CSDB_CallCDRProcessed"."CompanyId" = company
						and "CSDB_CallCDRProcessed"."TenantId" = tenant
						and "CSDB_CallCDRProcessed"."IsQueued" = true
						and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
						and "CSDB_CallCDRProcessed"."QueueSec" > 5
						and "CSDB_CallCDRProcessed"."AgentAnswered" = false
						and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
						and "CSDB_CallCDRProcessed"."AgentSkill" = any (
							regexp_split_to_array(
								skills,
								','
							)
						)
					group by
						c_date,
						c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
				) k on
				a."CompanyId" = k."CompanyId"
				and a."TenantId" = k."TenantId"
				and a."AgentSkill" = k."AgentSkill"
				and a.c_date = k.c_date
				and a.c_hour = k.c_hour
			left outer join (
					select
						avg( "QueueSec" ) as "answeredqueue_avg",
						timezone(
							'Asia/Colombo',
							"CSDB_CallCDRProcessed"."CreatedTime"
						)::date as c_date,
						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer as c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
					from
						"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
					where
						"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
												and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
						and "CSDB_CallCDRProcessed"."CompanyId" = company
						and "CSDB_CallCDRProcessed"."TenantId" = tenant
						and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
						and "CSDB_CallCDRProcessed"."AgentAnswered" = true
						and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
						and "CSDB_CallCDRProcessed"."AgentSkill" = any (
							regexp_split_to_array(
								skills,
								','
							)
						)
					group by
						c_date,
						c_hour,
						"CSDB_CallCDRProcessed"."AgentSkill",
						"CSDB_CallCDRProcessed"."CompanyId",
						"CSDB_CallCDRProcessed"."TenantId"
				) l on
				a."CompanyId" = l."CompanyId"
				and a."TenantId" = l."TenantId"
				and a."AgentSkill" = l."AgentSkill"
				and a.c_date = l.c_date
				and a.c_hour = l.c_hour
			)
		order by
			series.s_date,
			series.s_hour,
			a."AgentSkill";

else return query select
	a.ivrcount,
	b.queuedcount,
	c.abandonedcount,
	(
		c.abandonedcount * 100 / b.queuedcount
	)::numeric as abandonedpercent,
	j.droppedcount,
	(
		j.droppedcount * 100 / b.queuedcount
	)::numeric as droppedpercent,
	TO_CHAR(
		(
			d.holdsec_avg || ' second'
		)::interval,
		'HH24:MI:SS'
	),
	TO_CHAR(
		(
			e.ivrconnect_avg || ' second'
		)::interval,
		'HH24:MI:SS'
	),
	TO_CHAR(
		(
			f.answersec_avg || ' second'
		)::interval,
		'HH24:MI:SS'
	),
	TO_CHAR(
		(
			g.billsec_avg || ' second'
		)::interval,
		'HH24:MI:SS'
	),
	h.answered_count,
	TO_CHAR(
		(
			i.queuesec_avg || ' second'
		)::interval,
		'HH24:MI:SS'
	),
	(
		h.answered_count * 100 / b.queuedcount
	)::numeric as answeredpercent,
	TO_CHAR(
		(
			k.abandonedqueue_avg || ' second'
		)::interval,
		'HH24:MI:SS'
	),
	TO_CHAR(
		(
			l.answeredqueue_avg || ' second'
		)::interval,
		'HH24:MI:SS'
	),
	series.s_date,
	series.s_hour,
	a."AgentSkill",
	a."CompanyId",
	a."TenantId"
from
	(
		(
			select
				date_trunc(
					'day',
					dd
				):: date as s_date,
				date_part(
					'hour',
					dd
				)::integer as s_hour
			from
				generate_series (
					timezone(
						'Asia/Colombo',
						from_date
					) ,
					timezone(
						'Asia/Colombo',
						to_date
					) ,
					'1 hour'::interval
				) dd
		) as series
	left outer join (
			select
				count(*) as "ivrcount",
				timezone(
					'Asia/Colombo',
					"CSDB_CallCDRProcessed"."CreatedTime"
				)::date as c_date,
				date_part(
					'hour',
					timezone(
						'Asia/Colombo',
						"CSDB_CallCDRProcessed"."CreatedTime"
					)
				)::integer as c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
			from
				"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
			where
				"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
										and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
				and "CSDB_CallCDRProcessed"."CompanyId" = company
				and "CSDB_CallCDRProcessed"."TenantId" = tenant
				and "CSDB_CallCDRProcessed"."BusinessUnit" = b_unit
				and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
				and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
				and "CSDB_CallCDRProcessed"."AgentSkill" = any (
					regexp_split_to_array(
						skills,
						','
					)
				)
			group by
				c_date,
				c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
		) a on
		series.s_date = a.c_date
		and series.s_hour = a.c_hour
	left outer join (
			select
				count(*) as "queuedcount",
				timezone(
					'Asia/Colombo',
					"CSDB_CallCDRProcessed"."CreatedTime"
				)::date as c_date,
				date_part(
					'hour',
					timezone(
						'Asia/Colombo',
						"CSDB_CallCDRProcessed"."CreatedTime"
					)
				)::integer as c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
			from
				"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
			where
				"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
										and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
				and "CSDB_CallCDRProcessed"."BusinessUnit" = b_unit
				and "CSDB_CallCDRProcessed"."CompanyId" = company
				and "CSDB_CallCDRProcessed"."TenantId" = tenant
				and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
				and "CSDB_CallCDRProcessed"."IsQueued" = true
				and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
				and "CSDB_CallCDRProcessed"."AgentSkill" = any (
					regexp_split_to_array(
						skills,
						','
					)
				)
			group by
				c_date,
				c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
		) b on
		a."BusinessUnit" = b."BusinessUnit"
		and a."CompanyId" = b."CompanyId"
		and a."TenantId" = b."TenantId"
		and a."AgentSkill" = b."AgentSkill"
		and a.c_date = b.c_date
		and a.c_hour = b.c_hour
	left outer join (
			select
				count(*) as "abandonedcount",
				timezone(
					'Asia/Colombo',
					"CSDB_CallCDRProcessed"."CreatedTime"
				)::date as c_date,
				date_part(
					'hour',
					timezone(
						'Asia/Colombo',
						"CSDB_CallCDRProcessed"."CreatedTime"
					)
				)::integer as c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
			from
				"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
			where
				"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
										and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
				and "CSDB_CallCDRProcessed"."BusinessUnit" = b_unit
				and "CSDB_CallCDRProcessed"."CompanyId" = company
				and "CSDB_CallCDRProcessed"."TenantId" = tenant
				and "CSDB_CallCDRProcessed"."IsQueued" = true
				and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
				and "CSDB_CallCDRProcessed"."QueueSec" > 5
				and "CSDB_CallCDRProcessed"."AgentAnswered" = false
				and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
				and "CSDB_CallCDRProcessed"."AgentSkill" = any (
					regexp_split_to_array(
						skills,
						','
					)
				)
			group by
				c_date,
				c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
		) c on
		a."BusinessUnit" = c."BusinessUnit"
		and a."CompanyId" = c."CompanyId"
		and a."TenantId" = c."TenantId"
		and a."AgentSkill" = c."AgentSkill"
		and a.c_date = c.c_date
		and a.c_hour = c.c_hour
	left outer join (
			select
				avg( "HoldSec" ) as "holdsec_avg",
				timezone(
					'Asia/Colombo',
					"CSDB_CallCDRProcessed"."CreatedTime"
				)::date as c_date,
				date_part(
					'hour',
					timezone(
						'Asia/Colombo',
						"CSDB_CallCDRProcessed"."CreatedTime"
					)
				)::integer as c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
			from
				"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
			where
				"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
										and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
				and "CSDB_CallCDRProcessed"."HoldSec" > 0
				and "CSDB_CallCDRProcessed"."BusinessUnit" = b_unit
				and "CSDB_CallCDRProcessed"."CompanyId" = company
				and "CSDB_CallCDRProcessed"."TenantId" = tenant
				and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
				and "CSDB_CallCDRProcessed"."AgentAnswered" = true
				and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
				and "CSDB_CallCDRProcessed"."AgentSkill" = any (
					regexp_split_to_array(
						skills,
						','
					)
				)
			group by
				c_date,
				c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
		) d on
		a."BusinessUnit" = d."BusinessUnit"
		and a."CompanyId" = d."CompanyId"
		and a."TenantId" = d."TenantId"
		and a."AgentSkill" = d."AgentSkill"
		and a.c_date = d.c_date
		and a.c_hour = d.c_hour
	left outer join (
			select
				avg("IvrConnectSec") as "ivrconnect_avg",
				timezone(
					'Asia/Colombo',
					"CSDB_CallCDRProcessed"."CreatedTime"
				)::date as c_date,
				date_part(
					'hour',
					timezone(
						'Asia/Colombo',
						"CSDB_CallCDRProcessed"."CreatedTime"
					)
				)::integer as c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
			from
				"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
			where
				"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
										and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
				and "CSDB_CallCDRProcessed"."BusinessUnit" = b_unit
				and "CSDB_CallCDRProcessed"."CompanyId" = company
				and "CSDB_CallCDRProcessed"."TenantId" = tenant
				and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
				and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
				and "CSDB_CallCDRProcessed"."AgentSkill" = any (
					regexp_split_to_array(
						skills,
						','
					)
				)
			group by
				c_date,
				c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
		) e on
		a."BusinessUnit" = e."BusinessUnit"
		and a."CompanyId" = e."CompanyId"
		and a."TenantId" = e."TenantId"
		and a."AgentSkill" = e."AgentSkill"
		and a.c_date = e.c_date
		and a.c_hour = e.c_hour
	left outer join (
			select
				avg("AnswerSec") as "answersec_avg",
				timezone(
					'Asia/Colombo',
					"CSDB_CallCDRProcessed"."CreatedTime"
				)::date as c_date,
				date_part(
					'hour',
					timezone(
						'Asia/Colombo',
						"CSDB_CallCDRProcessed"."CreatedTime"
					)
				)::integer as c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
			from
				"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
			where
				"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
										and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
				and "CSDB_CallCDRProcessed"."BusinessUnit" = b_unit
				and "CSDB_CallCDRProcessed"."CompanyId" = company
				and "CSDB_CallCDRProcessed"."TenantId" = tenant
				and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
				and "CSDB_CallCDRProcessed"."AgentAnswered" = true
				and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
				and "CSDB_CallCDRProcessed"."AgentSkill" = any (
					regexp_split_to_array(
						skills,
						','
					)
				)
			group by
				c_date,
				c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
		) f on
		a."BusinessUnit" = f."BusinessUnit"
		and a."CompanyId" = f."CompanyId"
		and a."TenantId" = f."TenantId"
		and a."AgentSkill" = f."AgentSkill"
		and a.c_date = f.c_date
		and a.c_hour = f.c_hour
	left outer join (
			select
				avg("BillSec") as "billsec_avg",
				timezone(
					'Asia/Colombo',
					"CSDB_CallCDRProcessed"."CreatedTime"
				)::date as c_date,
				date_part(
					'hour',
					timezone(
						'Asia/Colombo',
						"CSDB_CallCDRProcessed"."CreatedTime"
					)
				)::integer as c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
			from
				"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
			where
				"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
										and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
				and "CSDB_CallCDRProcessed"."BusinessUnit" = b_unit
				and "CSDB_CallCDRProcessed"."CompanyId" = company
				and "CSDB_CallCDRProcessed"."TenantId" = tenant
				and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
				and "CSDB_CallCDRProcessed"."AgentAnswered" = true
				and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
				and "CSDB_CallCDRProcessed"."AgentSkill" = any (
					regexp_split_to_array(
						skills,
						','
					)
				)
			group by
				c_date,
				c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
		) g on
		a."BusinessUnit" = g."BusinessUnit"
		and a."CompanyId" = g."CompanyId"
		and a."TenantId" = g."TenantId"
		and a."AgentSkill" = g."AgentSkill"
		and a.c_date = g.c_date
		and a.c_hour = g.c_hour
	left outer join (
			select
				count(*) as "answered_count",
				timezone(
					'Asia/Colombo',
					"CSDB_CallCDRProcessed"."CreatedTime"
				)::date as c_date,
				date_part(
					'hour',
					timezone(
						'Asia/Colombo',
						"CSDB_CallCDRProcessed"."CreatedTime"
					)
				)::integer as c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
			from
				"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
			where
				"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
										and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
				and "CSDB_CallCDRProcessed"."BusinessUnit" = b_unit
				and "CSDB_CallCDRProcessed"."CompanyId" = company
				and "CSDB_CallCDRProcessed"."TenantId" = tenant
				and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
				and "CSDB_CallCDRProcessed"."AgentAnswered" = true
				and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
				and "CSDB_CallCDRProcessed"."AgentSkill" = any (
					regexp_split_to_array(
						skills,
						','
					)
				)
			group by
				c_date,
				c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
		) h on
		a."BusinessUnit" = h."BusinessUnit"
		and a."CompanyId" = h."CompanyId"
		and a."TenantId" = h."TenantId"
		and a."AgentSkill" = h."AgentSkill"
		and a.c_date = h.c_date
		and a.c_hour = h.c_hour
	left outer join (
			select
				avg("QueueSec") as "queuesec_avg",
				timezone(
					'Asia/Colombo',
					"CSDB_CallCDRProcessed"."CreatedTime"
				)::date as c_date,
				date_part(
					'hour',
					timezone(
						'Asia/Colombo',
						"CSDB_CallCDRProcessed"."CreatedTime"
					)
				)::integer as c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
			from
				"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
			where
				"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
										and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
				and "CSDB_CallCDRProcessed"."BusinessUnit" = b_unit
				and "CSDB_CallCDRProcessed"."CompanyId" = company
				and "CSDB_CallCDRProcessed"."TenantId" = tenant
				and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
				and "CSDB_CallCDRProcessed"."IsQueued" = true
				and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
				and "CSDB_CallCDRProcessed"."AgentSkill" = any (
					regexp_split_to_array(
						skills,
						','
					)
				)
			group by
				c_date,
				c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
		) i on
		a."BusinessUnit" = i."BusinessUnit"
		and a."CompanyId" = i."CompanyId"
		and a."TenantId" = i."TenantId"
		and a."AgentSkill" = i."AgentSkill"
		and a.c_date = i.c_date
		and a.c_hour = i.c_hour
	left outer join (
			select
				count(*) as "droppedcount",
				timezone(
					'Asia/Colombo',
					"CSDB_CallCDRProcessed"."CreatedTime"
				)::date as c_date,
				date_part(
					'hour',
					timezone(
						'Asia/Colombo',
						"CSDB_CallCDRProcessed"."CreatedTime"
					)
				)::integer as c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
			from
				"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
			where
				"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
										and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
				and "CSDB_CallCDRProcessed"."BusinessUnit" = b_unit
				and "CSDB_CallCDRProcessed"."CompanyId" = company
				and "CSDB_CallCDRProcessed"."TenantId" = tenant
				and "CSDB_CallCDRProcessed"."IsQueued" = true
				and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
				and "CSDB_CallCDRProcessed"."QueueSec" <= 5
				and "CSDB_CallCDRProcessed"."AgentAnswered" = false
				and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
				and "CSDB_CallCDRProcessed"."AgentSkill" = any (
					regexp_split_to_array(
						skills,
						','
					)
				)
			group by
				c_date,
				c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
		) j on
		a."BusinessUnit" = j."BusinessUnit"
		and a."CompanyId" = j."CompanyId"
		and a."TenantId" = j."TenantId"
		and a."AgentSkill" = j."AgentSkill"
		and a.c_date = j.c_date
		and a.c_hour = j.c_hour
	left outer join (
			select
				avg("QueueSec") as "abandonedqueue_avg",
				timezone(
					'Asia/Colombo',
					"CSDB_CallCDRProcessed"."CreatedTime"
				)::date as c_date,
				date_part(
					'hour',
					timezone(
						'Asia/Colombo',
						"CSDB_CallCDRProcessed"."CreatedTime"
					)
				)::integer as c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
			from
				"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
			where
				"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
										and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
				and "CSDB_CallCDRProcessed"."CompanyId" = company
				and "CSDB_CallCDRProcessed"."TenantId" = tenant
				and "CSDB_CallCDRProcessed"."IsQueued" = true
				and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
				and "CSDB_CallCDRProcessed"."QueueSec" > 5
				and "CSDB_CallCDRProcessed"."AgentAnswered" = false
				and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
				and "CSDB_CallCDRProcessed"."AgentSkill" = any (
					regexp_split_to_array(
						skills,
						','
					)
				)
			group by
				c_date,
				c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
		) k on
		a."BusinessUnit" = k."BusinessUnit"
		and a."CompanyId" = k."CompanyId"
		and a."TenantId" = k."TenantId"
		and a."AgentSkill" = k."AgentSkill"
		and a.c_date = k.c_date
		and a.c_hour = k.c_hour
	left outer join (
			select
				avg( "QueueSec" ) as "answeredqueue_avg",
				timezone(
					'Asia/Colombo',
					"CSDB_CallCDRProcessed"."CreatedTime"
				)::date as c_date,
				date_part(
					'hour',
					timezone(
						'Asia/Colombo',
						"CSDB_CallCDRProcessed"."CreatedTime"
					)
				)::integer as c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
			from
				"CSDB_CallCDRProcesseds" as "CSDB_CallCDRProcessed"
			where
				"CSDB_CallCDRProcessed"."CreatedTime" >= from_date
										and "CSDB_CallCDRProcessed"."CreatedTime" <= to_date
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer >= from_hour
						and 						date_part(
							'hour',
							timezone(
								'Asia/Colombo',
								"CSDB_CallCDRProcessed"."CreatedTime"
							)
						)::integer <= to_hour
				and "CSDB_CallCDRProcessed"."CompanyId" = company
				and "CSDB_CallCDRProcessed"."TenantId" = tenant
				and "CSDB_CallCDRProcessed"."DVPCallDirection" = 'inbound'
				and "CSDB_CallCDRProcessed"."AgentAnswered" = true
				and "CSDB_CallCDRProcessed"."ObjType" = 'HTTAPI'
				and "CSDB_CallCDRProcessed"."AgentSkill" = any (
					regexp_split_to_array(
						skills,
						','
					)
				)
			group by
				c_date,
				c_hour,
				"CSDB_CallCDRProcessed"."AgentSkill",
				"CSDB_CallCDRProcessed"."BusinessUnit",
				"CSDB_CallCDRProcessed"."CompanyId",
				"CSDB_CallCDRProcessed"."TenantId"
		) l on
		a."BusinessUnit" = l."BusinessUnit"
		and a."CompanyId" = l."CompanyId"
		and a."TenantId" = l."TenantId"
		and a."AgentSkill" = l."AgentSkill"
		and a.c_date = l.c_date
		and a.c_hour = l.c_hour
	)
order by
	series.s_date,
	series.s_hour,
	a."AgentSkill";

end if;

end
$BODY$;

ALTER FUNCTION public.hourly_call_summary_w_hour_f(timestamp with time zone, timestamp with time zone, integer, integer, character varying, character varying, character varying, integer, integer)
    OWNER TO postgres;

