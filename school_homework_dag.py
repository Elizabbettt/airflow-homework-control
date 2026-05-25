"""DAG для контроля домашних заданий в школе"""
import json
import random
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ---------- 1. ОПРЕДЕЛЯЕМ ФУНКЦИИ ДЛЯ ЗАДАЧ ----------

def generate_students(**context):
    """Генерирует список учеников"""
    students = [
        {"id": 1, "name": "Анна Иванова", "class": "10А"},
        {"id": 2, "name": "Борис Петров", "class": "10А"},
        {"id": 3, "name": "Виктория Сидорова", "class": "10Б"},
        {"id": 4, "name": "Глеб Козлов", "class": "10Б"},
        {"id": 5, "name": "Дарья Смирнова", "class": "10А"}
    ]
    context['ti'].xcom_push(key='students', value=students)
    print(f"Сгенерировано {len(students)} учеников")
    return students

def generate_teachers_subjects(**context):
    """Генерирует учителей и закреплённые за ними предметы"""
    teachers_subjects = {
        "Мария Ивановна": "Математика",
        "Пётр Сергеевич": "Русский язык",
        "Елена Андреевна": "Информатика",
        "Антон Владимирович": "История",
        "Ольга Дмитриевна": "Биология"
    }
    context['ti'].xcom_push(key='teachers_subjects', value=teachers_subjects)
    print(f"Сгенерировано {len(teachers_subjects)} учителей")
    return teachers_subjects

def generate_homework_assignments(**context):
    """Генерирует список домашних заданий"""
    assignments = [
        {"id": 1, "subject": "Математика", "description": "Решить №345-350", "deadline_days": 2},
        {"id": 2, "subject": "Русский язык", "description": "Написать сочинение", "deadline_days": 3},
        {"id": 3, "subject": "Информатика", "description": "Сделать проект на Python", "deadline_days": 5},
        {"id": 4, "subject": "История", "description": "Доклад о Петре I", "deadline_days": 4},
        {"id": 5, "subject": "Биология", "description": "Подготовить презентацию", "deadline_days": 3}
    ]
    context['ti'].xcom_push(key='assignments', value=assignments)
    print(f"Сгенерировано {len(assignments)} заданий")
    return assignments

def simulate_submissions_and_grades(**context):
    """Имитирует сдачу работ и проставление оценок"""
    students = context['ti'].xcom_pull(key='students', task_ids='generate_students')
    assignments = context['ti'].xcom_pull(key='assignments', task_ids='generate_assignments')
    
    results = []
    for student in students:
        for assignment in assignments:
            submitted = random.random() < 0.8
            if submitted:
                grade = random.choices([2, 3, 4, 5], weights=[0.1, 0.3, 0.4, 0.2])[0]
                status = "Сдано"
            else:
                grade = None
                status = "Не сдано"
            
            results.append({
                "student_id": student["id"],
                "student_name": student["name"],
                "assignment_id": assignment["id"],
                "subject": assignment["subject"],
                "submission_date": datetime.now().strftime("%Y-%m-%d"),
                "status": status,
                "grade": grade
            })
    
    context['ti'].xcom_push(key='results', value=results)
    print(f"Сгенерировано {len(results)} записей о сдаче работ")
    return results

def save_results_to_csv(**context):
    """Сохраняет результаты в CSV-файл"""
    results = context['ti'].xcom_pull(key='results', task_ids='simulate_submissions')
    df = pd.DataFrame(results)
    import os
    os.makedirs("/home/dnsstudent/airflow/dags/data", exist_ok=True)
    filename = f"/home/dnsstudent/airflow/dags/data/results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"Результаты сохранены в {filename}")
    return filename

def calculate_statistics(**context):
    """Рассчитывает статистику по сданным работам и оценкам"""
    results = context['ti'].xcom_pull(key='results', task_ids='simulate_submissions')
    df = pd.DataFrame(results)
    
    stats = {
        "total_submissions": len(df),
        "submitted_count": len(df[df['status'] == 'Сдано']),
        "not_submitted_count": len(df[df['status'] == 'Не сдано']),
        "average_grade": df[df['grade'].notna()]['grade'].mean() if len(df[df['grade'].notna()]) > 0 else 0,
    }
    
    import os
    os.makedirs("/home/dnsstudent/airflow/dags/data", exist_ok=True)
    stats_filename = f"/home/dnsstudent/airflow/dags/data/stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(stats_filename, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    
    print(f"\n=== СТАТИСТИКА ПО ДОМАШНИМ ЗАДАНИЯМ ===")
    print(f"Всего записей: {stats['total_submissions']}")
    print(f"Сдано работ: {stats['submitted_count']}")
    print(f"Не сдано: {stats['not_submitted_count']}")
    print(f"Средний балл: {stats['average_grade']:.2f}")
    print("="*40)
    return stats

# ---------- 2. ОПРЕДЕЛЯЕМ DAG ----------

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'school_homework_control',
    default_args=default_args,
    description='Контроль домашних заданий в школе',
    schedule='0 18 * * *',
    catchup=False,
    tags=['school', 'homework', 'education'],
)

# ---------- 3. ОПРЕДЕЛЯЕМ ЗАДАЧИ ----------

task_generate_students = PythonOperator(
    task_id='generate_students',
    python_callable=generate_students,
    dag=dag,
)

task_generate_teachers = PythonOperator(
    task_id='generate_teachers_subjects',
    python_callable=generate_teachers_subjects,
    dag=dag,
)

task_generate_assignments = PythonOperator(
    task_id='generate_assignments',
    python_callable=generate_homework_assignments,
    dag=dag,
)

task_simulate_submissions = PythonOperator(
    task_id='simulate_submissions',
    python_callable=simulate_submissions_and_grades,
    dag=dag,
)

task_save_csv = PythonOperator(
    task_id='save_results_to_csv',
    python_callable=save_results_to_csv,
    dag=dag,
)

task_calculate_stats = PythonOperator(
    task_id='calculate_statistics',
    python_callable=calculate_statistics,
    dag=dag,
)

task_echo = BashOperator(
    task_id='echo_completion',
    bash_command='echo "DAG school_homework_control успешно выполнен! $(date)"',
    dag=dag,
)

# ---------- 4. ЗАДАЁМ ПОРЯДОК ВЫПОЛНЕНИЯ ----------
task_generate_students >> task_generate_teachers >> task_generate_assignments
task_generate_assignments >> task_simulate_submissions
task_simulate_submissions >> [task_save_csv, task_calculate_stats]
task_save_csv >> task_echo
task_calculate_stats >> task_echo
