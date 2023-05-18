from flask import Flask, jsonify, request
from .S3Client import S3Client

app = Flask(__name__)

S3CLIENT = S3Client()


@app.route('/upload-csv', methods=['POST'])
def upload_csv():
    data = request.get_json()
    if not data:
        return jsonify({'message': 'Invalid payload'}), 400
    file_path = data.get('file_path')
    if not file_path:
        return jsonify({'message': 'file path is required'}), 400
    S3CLIENT.upload_file(file_path)
    return jsonify({'message': 'Record created successfully'})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
