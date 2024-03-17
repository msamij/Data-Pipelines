import json
import pathlib
import airflow
import requests
import requests.exceptions as requests_exceptions

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="download_rocket_launches",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)


# download_launches = BashOperator(
#     task_id="download_launches",
#     bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
#     dag=dag,
# )


data_dir_path = "GettingStarted/data"


def _download_launches():
    pathlib.Path(data_dir_path).mkdir(parents=True, exist_ok=True)
    try:
        respone = requests.get(
            url="https://ll.thespacedevs.com/2.0.0/launch/upcoming")
    except requests.exceptions.RequestException as e:
        raise e

    file_name = f"{data_dir_path}/launches.json"
    with open(file_name, "wb") as f:
        f.write(respone.content)


def _get_pictures():
    images_path = f"{data_dir_path}/images"
    pathlib.Path(images_path).mkdir(parents=True, exist_ok=True)

    with open(f"{data_dir_path}/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]

        for image_url in image_urls:
            try:
                response = requests.get(url=image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"{images_path}/{image_filename}"

                with open(target_file, "wb") as f:
                    f.write(response.content)
                    print(f"Downloaded {image_url} to {target_file}")

            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")


download_launches = PythonOperator(
    task_id="download_launches",
    python_callable=_download_launches,
    dag=dag
)

get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag,
)


notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls GettingStarted/data/images/ | wc -l) images."',
    dag=dag,
)

download_launches >> get_pictures >> notify
