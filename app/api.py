from flask import Flask, jsonify, request, send_file
from app.s3 import S3Client
from .errors import UploadingFileError, DownloadFileError, \
        FileNotFoundError, DeletingFileError, UpdatingFileError

app = Flask(__name__)

S3CLIENT = S3Client()


@app.route('/upload-csv', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided.'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'Invalid file name.'}), 400
    if file.filename.endswith('.csv'):
        try:
            S3CLIENT.upload_file(file, file.filename)
            return jsonify({'message': 'Record created successfully'}), 200
        except UploadingFileError:
            return jsonify({'error': 'Error uploading .csv file'}), 500
    else:
        return jsonify({'error': 'Invalid file. Please upload a .csv'}), 400


@app.route('/download-csv/<filename>', methods=['GET'])
def download_csv(filename):
    if not filename:
        return jsonify({'error': 'No filename provided'}), 400
    file_path = f'/tmp/{filename}'

    try:
        S3CLIENT.download_file(file_path)
        return send_file(
                file_path,
                download_name=filename
                )
    except FileNotFoundError:
        return jsonify({'error': 'File not found'}), 400
    except DownloadFileError:
        return jsonify({'error': 'Error downloading file'}), 500


@app.route('/delete-csv/<filename>', methods=['DELETE'])
def delete_csv(filename):
    if not filename:
        return jsonify({'error': 'No filename provided'}), 400
    if not filename.endswith('.csv'):
        return jsonify({
            'error': 'Invalid file format. Only CSV files are allowed.'
            }), 400
    try:
        S3CLIENT.delete_file(filename)
        return jsonify({'message': 'Record deleted successfully'}), 200
    except FileNotFoundError:
        return jsonify({'error': 'File not found'}), 400
    except DeletingFileError:
        return jsonify({'error': 'Error deleting file'}), 500


@app.route('/update-csv/<filename>', methods=['PUT'])
def update_csv(filename):
    if not filename:
        return jsonify({'error': 'No filename provided'}), 400
    if not filename.endswith('.csv'):
        return jsonify({
            'error': 'Invalid file name.'
            }), 400

    if 'file' not in request.files:
        return jsonify({'error': 'No file provided.'}), 400

    file = request.files['file']

    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'Invalid file. Please upload a .csv'}), 400

    try:
        S3CLIENT.update_file(file, filename)
        return jsonify({'message': f'{filename} updated successfully'}), 200
    except FileNotFoundError:
        return jsonify({'error': 'File not found error'}), 400
    except UpdatingFileError:
        return jsonify({'error': f'Error updating {filename} file'}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
