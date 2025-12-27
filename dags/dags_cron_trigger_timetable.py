from airflow.timetables.trigger import CronTriggerTimetable
from airflow.sdk import DAG, task
from datetime import timedelta
from pprint import pprint

with DAG(
		dag_id='dags_cron_trigger_timetable',
		schedule=CronTriggerTimetable( #해당 라이브러리가 config 설정을 true로 잡아도, datainterval을 고려하지 않음
					cron='*/5 * * * *',
					timezone='Asia/Seoul',
					interval=timedelta(minutes=5) ##주석처리하거나, 그냥 써보거나.. #인터벌 강제 주입
		),
		tags=['cron']
) as dag :

	@task(task_id='task_show_context1')
	def task_show_context1(**context):
		print('::group::show context variables')
		pprint(context)
		print('::endgroup::')
	
	task_show_context1()