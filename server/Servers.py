import os
from flask import Flask, jsonify, request
import json

app = Flask(__name__)


@app.route("/api/tohive", methods=['POST', ])
def tohive():
    """
    数据写入hive

    :return:
    """
    data = request.json.get("data", '')
    table = request.json.get("table", '')
    data = json.dumps(data)
    command = f"""spark-submit  --master yarn  --deploy-mode cluster  useSparkToHIVE.py '{table}' '{data}'  """
    df = os.popen(command)
    res = df.read()
    return jsonify({'code': '200', "data": str(res)})


@app.route("/api/task", methods=['POST', ])
def task():
    """
    执行调度运行 spark job
    date: YY-MM-DD
    tablename: dws_dwsxxxx
    active:

    :return:
    """
    date = request.json.get("date", '2023-02-02')
    tablename = request.json.get("tablename", '')
    active = request.json.get("active", '')
    database = str(tablename).split("_", 1)[0]

    if active == "spark":
        command = f"sh /home/airflow/airflow/sources/mas/spark/table/{database}/{tablename}.sh {date} feature"
    elif active == "sqoop":
        command = f"sh /home/airflow/airflow/sources/mas/sqoop/shell/{tablename}.sh {date} feature"
    elif active == "starrock":
        command = f"sh /home/airflow/airflow/sources/mas/starrocks/table/{tablename}.sh {date} "
    else:
        return jsonify({'code': '400', "data": "active 参数错误，('spark', 'starrock', 'sqoop')"})

    os.system(command)

    return jsonify({'code': '200', "data": ""})


@app.route("/api/hello", methods=['GET', ])
def hello():
    """
    数据写入hive

    :return:
    """

    return jsonify({'code': '200', "data": str("1111111")})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=16888)
