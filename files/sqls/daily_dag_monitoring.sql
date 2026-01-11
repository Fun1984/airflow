with today_dag as (
	SELECT 
			dag_id
		 ,next_dagrun_data_interval_start
		 ,next_dagrun_data_interval_end
		FROM dag
		where is_paused = false
		  and is_stale = false
		  and timetable_summary not in ('None', '"Asset"', '"None"')
		  and (date(next_dagrun_data_interval_start) between current_date -1 and current_date
		   or date(next_dagrun_data_interval_end) between current_date -1 and current_date)
)
, today_dagrun as (
	SELECT 
			dag_id
		 ,count(1) as run_cnt
		 ,count(case when state='success' then 'success' end) as success_cnt
		 ,count(case when state='failed' then 'failed' end) as failed_cnt
		 ,count(case when state='running' then 'running' end) as running_cnt
		 ,max(case when state='failed' then data_interval_end end) as last_failed_date
		 ,max(case when state ='success' then data_interval_end end) as last_success_date
		FROM dag_run
	 WHERE date(data_interval_end) between current_date -1 and current_date
	 GROUP BY dag_id
)
SELECT
		d.dag_id
	 ,r.run_cnt
	 ,r.success_cnt
	 ,r.failed_cnt
	 ,r.running_cnt
	 ,r.last_failed_date
	 ,r.last_success_date
	 ,next_dagrun_data_interval_start
	 ,next_dagrun_data_interval_end
	FROM today_dag d
	left join today_dagrun r
				ON d.dag_id = r.dag_id