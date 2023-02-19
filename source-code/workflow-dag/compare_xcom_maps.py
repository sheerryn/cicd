import re


from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage


def submit_job(project_id, region, cluster_name):
    # Create the job client.
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the job config. 'main_jar_file_uri' can also be a
    # Google Cloud Storage URL.
    job = {
        "placement": {"cluster_name": cluster_name},
        "spark_job": {
            "main_class": "org.apache.spark.examples.SparkPi",
            "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest.jar"], 
            "args": ["1000"],
        },
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()

    # Dataproc job output gets saved to the Google Cloud Storage bucket
    # allocated to the job. Use a regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_string()
    )

    print(f"Job finished successfully: {output}")
    
  if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit("python sales.py project_id region cluster_name")

    project_id = ${GCP_PROJECT_ID}
    region = ${REGION}
    cluster_name = ${DATAPROC_CLUSTER_NAME}
    submit_job(project_id, region, cluster_name)
