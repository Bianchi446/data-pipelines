# DAG for downloading and processing rocket launch data 

import json
import pathlib

import airflow
import requests 
import requests.exceptions as requests_exceptions 

from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
            dag_id = "rocket_launch_data",
            start_date = airflow.utils.dates.days_ago(14),
            schedule_interval = None,
        )

download_launches = BashOperator(
            task_id="download_launches",
            bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
            dag=dag,
        )


def _get_pictures():

    # Ensure directory exists 
    pathlib.Path("/tmp/launches.json").mkdir(parents=True, exists_ok = True)
    
    # download all pictures in launches.json 
    with open("/tmp/launches.json") as f: 
        images_urls = [launch["images"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema: 
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connet to {image_url}.")

    get_pictures = PythonOperator(
        
        task_id = "get_pictures",
        python_callable = "get_pictures",
        dag=dag,
            )

    notify = BashOperator(

        task_id = "notify",
        bash_command='echo "There are now $(ls /tmp/images/ | wc -1) images."',
        dag=dag,
            )

    download_launches >> get_pictures >> notify




