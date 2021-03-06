create table if not exists agent_productivity_summary
(
	summary_date date,
	tenant integer,
	company integer,
	bu text,
	agent integer,
	login_max_time integer,
	login_total_count bigint,
	login_total_time text,
	login_time timestamp with time zone,
	inbound_max_time integer,
	inbound_total_count bigint,
	inbound_total_time text,
	outbound_max_time integer,
	outbound_total_count bigint,
	outbound_total_time text,
	total_call_count bigint,
	total_call_time bigint,
	avg_inbound_call_count numeric,
	avg_outbound_call_count numeric,
	avg_inbound_call_time numeric,
	avg_outbound_call_time numeric,
	inbound_talk_max_time integer,
	inbound_connected_total_count bigint,
	inbound_talk_total_time text,
	outbound_talk_max_time integer,
	outbound_connected_total_count bigint,
	outbound_talk_total_time text,
	connected_total_count bigint,
	total_talk_total_time bigint,
	avg_inbound_talk_time text,
	avg_outbound_talk_time text,
	avg_inbound_handling_time text,
	avg_outbound_handling_time text,
	inbound_hold_max_time integer,
	inbound_hold_total_count bigint,
	inbound_hold_total_time text,
	outbound_hold_max_time integer,
	outbound_hold_total_count bigint,
	outbound_hold_total_time text,
	total_hold_total_count bigint,
	total_hold_total_time bigint,
	avg_inbound_hold_count numeric,
	avg_outbound_hold_count numeric,
	avg_inbound_hold_time text,
	avg_outbound_hold_time text,
	inbound_acw_max_time integer,
	inbound_acw_total_count bigint,
	inbound_acw_total_time text,
	outbound_acw_max_time integer,
	outbound_acw_total_count bigint,
	outbound_acw_total_time text,
	total_acw_total_count bigint,
	total_acw_total_time bigint,
	avg_inbound_acw_count numeric,
	avg_outbound_acw_count numeric,
	avg_inbound_acw_time numeric,
	avg_outbound_acw_time numeric,
	idle_time_inbound text,
	idle_time_outbound text,
	idle_time_offline text,
	total_break_time text,
	full_total_login_time text,
	full_total_inbound_time text,
	full_total_outbound_time text,
	full_total_inbound_idle_time text,
	full_total_outbound_idle_time text,
	full_total_offline_idle_time text,
	full_total_inbound_acw_time text,
	full_total_outbound_acw_time text,
	full_total_inbound_talk_time text,
	full_total_outbound_talk_time text,
	full_total_inbound_hold_time text,
	full_total_outbound_hold_time text,
	full_total_inbound_hold_count bigint,
	full_total_outbound_hold_count bigint,
	full_total_break_time text,
	full_total_connected_inbound_calls bigint,
	full_total_connected_outbound_calls bigint,
	full_total_inbound_calls bigint,
	full_total_outbound_calls bigint,
	full_avg_inbound_handling_time text,
	full_avg_outbound_handling_time text,
	full_avg_inbound_talk_time text,
	full_avg_outbound_talk_time text,
	full_avg_inbound_hold_time text,
	full_avg_outbound_hold_time text
);

alter table agent_productivity_summary owner to duo;

