"""
An Airflow Python script is really just a configuration file specifying a DAG structure
as code
"""
import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["henry.ehly@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    "foobar",
    description="A simple tutorial DAG",
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
    # if templates are located in a directory other than Path(__file__).parent
    # template_searchpath=
    user_defined_filters={"hello": lambda name: "Hello %s" % name},
)

t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

# Test with:
# poetry run airflow test foobar sleep 2015-06-01
# the "2015-06-01" is required and tells airflow to simulate that the task executed
# on that date
t2 = BashOperator(
    task_id="sleep", depends_on_past=False, bash_command="sleep 5", retries=3, dag=dag
)

templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

# Test with:
# poetry run airflow test foobar templated 2015-06-01
t3 = BashOperator(
    task_id="templated",
    depends_on_past=False,
    bash_command=templated_command,
    params={"my_param": "Parameter I passed in"},
    dag=dag,
)

# Test with:
# poetry run airflow test foobar foo 2015-06-01
t4 = BashOperator(
    task_id="foo",
    depends_on_past=False,
    bash_command="external_script.sh",
    params={"my_other_param": "Another parameter I passed in"},
    dag=dag,
)

dag.doc_md = __doc__

t1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

#
# Set task dependencies
#

# invoke t2 after t1 completes successfully
t1.set_downstream(t2)

# the bit shift operator also specifies dependencies
# here we say invoke task 2 after task 1
t1 >> t2

# we can flip it around too
t2.set_upstream(t1)
t2 << t1

# AIRFLOW__CORE__DAGS_FOLDER=/path/to/dags poetry run airflow list_tasks foobar --tree
# <Task(BashOperator): print_date>
#     <Task(BashOperator): foo>
#     <Task(BashOperator): sleep>
#     <Task(BashOperator): templated>
t1 >> [t2, t3, t4]

# List your dags
# AIRFLOW__CORE__DAGS_FOLDER=/path/to/dags poetry run airflow list_dags

# backfill this dag
# meaning execute it once per day (default?) from start_date (-s) to end_date (-e)
# so the above dependency will happen 3 times (3/2, 3/3, 3/4)
# poetry run airflow backfill foobar -s 2020-03-02 -e 2020-03-04
