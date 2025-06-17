import hashlib
import logging
import os
from functools import wraps
from typing import Optional

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from flask import Flask, abort, jsonify, request
from mypy_boto3_s3.client import S3Client

app = Flask(__name__)

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

s3: S3Client = boto3.client(
    "s3",
    endpoint_url=os.getenv("ENDPOINT_URL"),
    region_name=os.getenv("REGION_NAME"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    config=Config(signature_version="s3v4"),
)
file_service_secret_key = os.getenv("SECRET_KEY")

bucket_name = os.getenv("BUCKET_NAME")


def object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "404":
            return False
        raise


def get_object_content(key: str) -> Optional[str]:
    try:
        if not bucket_name:
            raise RuntimeError("BUCKET_NAME is not set")
        response = s3.get_object(Bucket=bucket_name, Key=key)
        return response["Body"].read().decode("utf-8")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "404":
            return None
        raise


def hash_string(data: str, algorithm: str = "sha256") -> str:
    hash_obj = hashlib.new(algorithm)

    hash_obj.update(data.encode("utf-8"))
    # Получаем хэш-сумму в виде шестнадцатеричной строки
    return hash_obj.hexdigest()


def requires_secret_key(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        secret_key = request.headers.get("key")

        if secret_key != file_service_secret_key:
            return jsonify({"error": "Invalid secret key"}), 403

        return f(*args, **kwargs)

    return decorated_function


# Маршрут для проверки статуса приложения
@app.route("/")
def home():
    return "The service is running."


@app.route("/get-object/<file_id>", methods=["GET"])
@requires_secret_key
def get_request(file_id):
    # Получаем имя объекта из параметров запроса
    # file_id = request.args.get('file_id')

    # Отладочный вывод
    logger.info(f"Received request with file_id: {file_id}")

    if not file_id:
        abort(400, description="Параметр 'file_id' обязателен.")
    if not bucket_name:
        raise RuntimeError("BUCKET_NAME is not set")
    if not object_exists(bucket_name, file_id):
        abort(404, description="Объект не найден.")

    # Получаем содержимое объекта
    content = get_object_content(file_id)

    # Возвращаем содержимое объекта в виде ответа
    return jsonify({"content": content})


@app.route("/post-object", methods=["POST"])
@requires_secret_key
def post_request():
    try:
        logger.info("📩 Получен POST /post-object")

        data = request.json
        if not data:
            logger.info("⛔️ Нет JSON в теле запроса")
            return jsonify({"error": "No JSON data provided"}), 400

        b64_string = data.get("data_base64")
        extension = data.get("ext")

        logger.info(f"📄 Расширение: {extension}")
        logger.info(
            f"🔠 Base64 длина строки: {len(b64_string) if b64_string else 'None'}"
        )

        if not b64_string or not extension:
            logger.info("⛔️ Не хватает ключей 'data_base64' или 'ext'")
            return jsonify({"error": "Missing required fields"}), 400

        hash = hash_string(b64_string)
        s3_file_key = f"{hash}.{extension}"

        logger.info(f"🔑 S3 ключ: {s3_file_key}")

        if not bucket_name:
            raise RuntimeError("BUCKET_NAME is not set")

        logger.info(f"🪣 Проверяем наличие объекта: {bucket_name}/{s3_file_key}")
        exists = object_exists(bucket_name, s3_file_key)
        logger.info(f"📦 Объект уже существует? {'Да' if exists else 'Нет'}")

        if not exists:
            logger.info("📤 Загружаем объект в S3...")
            s3.put_object(
                Bucket=bucket_name,
                Key=s3_file_key,
                Body=b64_string.encode("utf-8"),
                ContentType="text/plain",
            )
            logger.info("✅ Объект загружен")
            return jsonify({"status": "created", "data": s3_file_key})
        else:
            logger.info("ℹ️ Объект уже существует")
            return jsonify({"status": "exists", "data": s3_file_key})

    except Exception as e:
        logger.info(f"🔥 Ошибка: {type(e).__name__}: {str(e)}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5002)
