import os
import pandas as pd
import sqlalchemy as sa
from flask import Flask, request, jsonify

app = Flask(__name__)

UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def connect_to_db(db_name, username, password, host='localhost'):
    engine = sa.create_engine(f'mysql+pymysql://{username}:{password}@{host}/{db_name}')
    conn = engine.connect()
    return conn

def get_data_from_db(conn, table_name):
    query = f'SELECT * FROM {table_name}'
    df = pd.read_sql_query(query, conn)
    return df

def data_profiling(data):
    if isinstance(data, str):
        if data.endswith('.csv'):
            df = pd.read_csv(data)
        elif data.endswith(('.xlsx', '.XLSX')):
            df = pd.read_excel(data)
        else:
            return jsonify({'error': 'Unsupported file format'})
    else:
        df = data

    profile = {
        "Number of Rows": len(df),
        "Number of Columns": len(df.columns),
        "Columns": df.columns.tolist(),
    }

    data_types = df.dtypes.value_counts().to_dict()
    data_type_info = {}
    for dtype, count in data_types.items():
        data_type_info[str(dtype)] = count
    profile["Data Types"] = data_type_info

    missing_values = df.isnull().sum().to_dict()
    profile["Missing Values"] = missing_values

    statistics = df.describe().to_dict()
    stats_info = {}
    for col, values in statistics.items():
        stats_info[col] = {}
        for key, value in values.items():
            stats_info[col][key] = value
    profile["Basic Statistics"] = stats_info

    return profile

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' in request.files:
        file = request.files['file']
        if file.filename != '':
            if file.filename.endswith(('.csv', '.xlsx', '.XLSX')):
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], file.filename))
                profile_result = data_profiling(os.path.join(app.config['UPLOAD_FOLDER'], file.filename))
                return jsonify(profile_result)
            else:
                return jsonify({'error': 'Unsupported file format'})
    
    elif 'db_name' in request.form and 'username' in request.form and 'password' in request.form and 'table_name' in request.form:
        db_name = request.form['db_name']
        username = request.form['username']
        password = request.form['password']
        table_name = request.form['table_name']
        
        conn = connect_to_db(db_name, username, password)
        df = get_data_from_db(conn, table_name)
        profile_result = data_profiling(df)
        return jsonify(profile_result)

    return jsonify({'error': 'Invalid request'})

if __name__ == '__main__':
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    app.run(debug=True)
