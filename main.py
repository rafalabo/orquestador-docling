import os
import boto3
import requests

# 1. CONFIGURACIÓN (Usa variables de entorno por seguridad)
COS_ENDPOINT = "https://s3.private.us-east.cloud-object-storage.appdomain.cloud" # Red privada
COS_API_KEY = os.getenv("COS_API_KEY")
COS_INSTANCE_ID = os.getenv("COS_INSTANCE_ID")
BUCKET_IN = "1-raw-docs"
BUCKET_MID = "2-mid-docs"
DOCLING_URL = os.getenv("DOCLING_URL") # Tu URL de Code Engine

# 2. CLIENTE ICOS
cos = boto3.client("s3",
    ibm_api_key_id=COS_API_KEY,
    ibm_service_instance_id=COS_INSTANCE_ID,
    endpoint_url=COS_ENDPOINT,
    config=boto3.session.Config(signature_version="oauth")
)

def run_pipeline():
    # Listar archivos
    response = cos.list_objects_v2(Bucket=BUCKET_IN)
    for obj in response.get('Contents', []):
        file_name = obj['Key']
        if not file_name.endswith(".pdf"): continue

        print(f"Procesando: {file_name}")

        # Generar URL Firmada (Interna)
        signed_url = cos.generate_presigned_url('get_object', 
            Params={'Bucket': BUCKET_IN, 'Key': file_name}, ExpiresIn=600)

        # Enviar a Docling
        res = requests.post(f"{DOCLING_URL}/convert", json={"url": signed_url})
        
        if res.status_code == 200:
            markdown = res.json().get("document", {}).get("markdown", "")
            # Subir a Bucket Intermedio
            output_name = file_name.replace(".pdf", ".md")
            cos.put_object(Bucket=BUCKET_MID, Key=output_name, Body=markdown)
            print(f"Guardado: {output_name}")

if __name__ == "__main__":
    run_pipeline()