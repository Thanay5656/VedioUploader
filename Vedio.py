import os
import sqlite3
import uuid
import secrets
from flask import Flask, request, jsonify, render_template, abort
import boto3
from botocore.exceptions import ClientError
app = Flask(__name__)
app.secret_key = secrets.token_hex(32)
BUCKET = os.environ.get('S3_BUCKET')
if not BUCKET:
    raise RuntimeError("Environment variable S3_BUCKET is not set. Please set it to your AWS S3 bucket name.")
conn = sqlite3.connect('videos.db', check_same_thread=False)
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS videos (
    id TEXT PRIMARY KEY,
    s3_key TEXT NOT NULL
)
''')
conn.commit()
s3 = boto3.client('s3')

def create_presigned_put(key, expires=3600):
    try:
        return s3.generate_presigned_url(
            'put_object',
            Params={'Bucket': BUCKET, 'Key': key},
            ExpiresIn=expires,
            HttpMethod='PUT'
        )
    except ClientError:
        return None

def create_presigned_get(key, expires=3600):
    try:
        return s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': BUCKET, 'Key': key},
            ExpiresIn=expires
        )
    except ClientError:
        return None

@app.route('/')
def dashboard():
    return render_template('upload.html')

@app.route('/generate-upload-url', methods=['POST'])
def generate_upload_url():
    ext = request.json.get('extension', '.mp4')
    key = f"{uuid.uuid4()}{ext}"
    url = create_presigned_put(key)
    if not url:
        return jsonify(error="Failed to generate upload URL"), 500
    return jsonify(upload_url=url, s3_key=key)

@app.route('/confirm-upload', methods=['POST'])
def confirm_upload():
    key = request.json.get('s3_key')
    if not key:
        return abort(400, "Missing s3_key")
    vid_id = str(uuid.uuid4())
    cursor.execute("INSERT INTO videos (id, s3_key) VALUES (?, ?)", (vid_id, key))
    conn.commit()
    return jsonify(video_id=vid_id)

@app.route('/videos')
def list_videos():
    cursor.execute("SELECT id, s3_key FROM videos")
    result = cursor.fetchall()
    videos = [{'id': vid, 'play_url': create_presigned_get(s3k)} for vid, s3k in result]
    return jsonify(videos)

if __name__ == '__main__':
    app.run(debug=True)

