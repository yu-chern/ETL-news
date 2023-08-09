# ETL-news
## Contributors
* Yu Chen
* Zeqing Cai
# A Simple ETL Data Pipeline: Extract Data from Raw News Source and Save in A Database
![Airflow ETL Data Pipeline](fig/data_pipeline.jpg)
## Requirements
* System -- MacOS or Linux. If you use Windows, it is recommended to set up every thing in WSL2 Ubuntu 22.04
* Database system -- Postgres. Please install Postgres database system on your machine.
* Virtual Environment -- 
  1. Install python3-pip first on your machine
  2. Install virtualenv
    ```Shell
        username@desktop_name:~/airflow$ pip3 install virtualenv
    ```
  3. Create a virtualenv called `airflow_env` where all required Python packages will be installed
    ```
        username@desktop_name:~/airflow$ virtualenv airflow_env
    ```
  4. Activate `airflow_env`
    ```
        username@desktop_name:~/airflow$ source airflow_env/bin/activate
    ```
* Install Python libraries after activating `airflow_env`
    ```
        (airflow_env) username@desktop_name:~/airflow$ pip install pandas
        (airflow_env) username@desktop_name:~/airflow$ pip install 'apache-airflow==2.6.2' \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.2/constraints-3.8.txt"
        (airflow_env) username@desktop_name:~/airflow$ pip install psycopg2-binary
        (airflow_env) username@desktop_name:~/airflow$ pip install apache-airflow-providers-postgres
    ```

## Use Airflow
* Create `dags` folder
    ```
        (airflow_env) username@desktop_name:~/airflow$ mkdir dags
    ```
* Initialize airflow db. This will create a `airflow.cfg` file.
    ```
        (airflow_env) username@desktop_name:~/airflow$ airflow db init
    ```
* Edit `airflow.cfg` to configure airflow. It is only recommanded to only change the following three terms `load_examples`, `sqp_alchemy_conn`, and `executor`, if you do not understand much.
    ```
        load_examples = False
        sql_alchemy_conn = postgresql+psycopg2://postgres:passcode_of_postgres_user@localhost:5432/postgres
        executor = LocalExecutor
    ```
* Restart airflow db to activated the configuration on the `airflow.cfg` file.
    ```
        (airflow_env) username@desktop_name:~/airflow$ airflow db init
    ```
* Create the `admin` user with your information
    ```
        (airflow_env) username@desktop_name:~/airflow$ airflow users create --username admin --password your_password --firstname your_first_name --lastname your_last_name --role Admin --email your_email@some.com
    ```
## Use the ETL data pipeline

* Put `etl_pipeline.py`, `news_extractor.py`, `transform.py` into the `~/airflow/dags/`.
    
* Run airflow `scheduler` with the following command in one terminal with `airflow_env` activated
    ```
        (airflow_env) username@desktop_name:~/airflow$ airflow scheduler
    ```
* Run airflow `webserver` with the following command in another terminal `airflow_env` activated
    ```
        (airflow_env) username@desktop_name:~/airflow$ airflow webserver
    ```
* Open a web browser and type `localhost:8080` and enter. You will be led to log in Airflow. Input `admin` for username and your `passcode` for the `admin` user to login.
  
* Refresh and you will see the follwing:
    
    ![Airflow ETL Data Pipeline](fig/airflow_dags.jpg)
    
* Unpose the `etl_news_pipeline` dag to activate it and Airflow will run each task as you defined and scheduled

    ![Airflow ETL Data Pipeline](fig/airflow_event_management.jpg)


