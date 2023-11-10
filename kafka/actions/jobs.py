from typing import List, Any, Optional
from pydantic import BaseModel
from . import action_store as action_store
from .http_wrapper import get_wrapper, post_wrapper, post_wrapper_full_response, get_wrapper_full_response
import time
import logging

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

class BuildParams(BaseModel):
    job_name: str
    build_number: str

class BuildConsoleLogsRequest(BaseModel):
    job_name: str
    build_number: int

class BuildConsoleLogsResponse(BaseModel):
    console_logs: str

class TriggerJobRequest(BaseModel):
    job_name: str
    parameters: Optional[str] = None

class TriggerJobResponse(BaseModel):
    success: bool
    message: str
    build_number: Optional[str] = None

class JobStatusRequest(BaseModel):
    job_name: str
    build_number: Optional[str] = None

class JobStatusResponse(BaseModel):
    status: str
    message: str
    build_logs: Optional[str] = None

class ArtifactRequest(BaseModel):
    job_name: str
    build_number: int
    artifact_path: str

class ArtifactResponse(BaseModel):
    artifact: str


@action_store.kubiya_action()
def get_job_status(request: JobStatusRequest) -> JobStatusResponse:
    logger.info(f"Getting status for job {request.job_name}")
    if request.build_number:
        logger.info(f"Build number provided, getting status for specific build with ID {request.build_number}")
        endpoint = f"/job/{request.job_name}/{request.build_number}/api/json?tree=result"
    else:
        logger.info("Build number not provided, getting status for last build")
        endpoint = f"/job/{request.job_name}/lastBuild/api/json?tree=result"
    try:
        response = get_wrapper(endpoint)
        status = response["result"]
        message = "Job is in progress."
        logger.info(f"Job {request.job_name} status: {status}")
        if status in ["SUCCESS", "FAILURE"]:
            logger.info(f"Job {request.job_name} completed with status {status}")
            message = f"Job status: {status}"
        return JobStatusResponse(status=status, message=message)
    except Exception as e:
        logger.error(f"Error getting status for job {request.job_name}: {e}")
        return JobStatusResponse(status="", message=str(e))


# Suggested to run this action asynchronously
@action_store.kubiya_action()
def wait_for_job_completion(params: BuildParams) -> JobStatusResponse:
    logger.info(f"Waiting for job {params.job_name} with build number {params.build_number} to complete")
    max_retries = 10  # Maximum number of retries
    retry_delay = 5  # Delay between retries in seconds
    timeout = 300  # Timeout in seconds (e.g., 5 minutes)

    start_time = time.time()
    retries = 0

    while True:
        elapsed_time = time.time() - start_time

        if elapsed_time > timeout:
            logger.error(f"Timeout reached while waiting for job {params.job_name} to complete.")
            return JobStatusResponse(status="", message="Timeout reached while waiting for job completion.")

        if retries >= max_retries:
            logger.error(f"Exceeded maximum number of retries while waiting for job {params.job_name} to complete.")
            return JobStatusResponse(status="", message="Exceeded maximum number of retries while waiting for job completion.")

        job_status = get_job_status(JobStatusRequest(job_name=params.job_name, build_number=params.build_number))
        if job_status.status in ["SUCCESS", "FAILURE"]:
            logger.info(f"Job {params.job_name} completed with status {job_status.status}")
            build_logs = get_build_console_logs(BuildConsoleLogsRequest(job_name=params.job_name, build_number=params.build_number))
            return JobStatusResponse(status=job_status.status, message=job_status.message, build_logs=build_logs.console_logs)

        retries += 1
        time.sleep(retry_delay)


def get_build_number_from_queue(queue_location: str, job_name: str) -> Optional[int]:
    for _ in range(10):  # retry for 10 times
        # Query the queue API for the build number
        queue_response = get_wrapper_full_response(queue_location)
        if queue_response.status_code == 200:
            queue_data = queue_response.json()
            if 'executable' in queue_data:
                return queue_data['executable']['number']
        elif queue_response.status_code == 404:
            # Job has left the queue
            break
        time.sleep(2)  # wait for 2 seconds before retrying
    return None


def get_latest_build_number(job_name: str) -> Optional[int]:
    # Job has left the queue, query job's builds API for the latest build number
    job_response = get_wrapper_full_response(f"/job/{job_name}/api/json")
    job_data = job_response.json()
    if 'builds' in job_data and job_data['builds']:
        return job_data['builds'][0]['number']
    return None


@action_store.kubiya_action()
def trigger_job(request: TriggerJobRequest) -> TriggerJobResponse:
    logger.info(f"Triggering job {request.job_name}")
    endpoint = f"/job/{request.job_name}/buildWithParameters{request.parameters}" if request.parameters else f"/job/{request.job_name}/build"
    try:
        response = post_wrapper_full_response(endpoint)
        if response.status_code != 201:
            return TriggerJobResponse(success=False, message=f"Unexpected response status code: {response.status_code}",
                                      build_number=None)

        # Get the queue location from the response headers
        queue_location = response.headers.get('Location')
        logger.info(f"Job {request.job_name} triggered successfully.")
        if queue_location is None:
            return TriggerJobResponse(success=False, message="Queue location not found in response headers.",
                                      build_number=None)

        # Try to get build number from the queue
        build_number = get_build_number_from_queue(queue_location, request.job_name)

        retry_count = 5
        delay = 10  # delay in seconds
        while build_number is None and retry_count > 0:
            logger.info(f"Job {request.job_name} has left the queue - retrying to get latest build number.")
            time.sleep(delay)  # wait for some time before trying again
            build_number = get_latest_build_number(request.job_name)
            retry_count -= 1

        if build_number is not None:
            logger.info(f"Job {request.job_name} build number: {build_number}")
            return TriggerJobResponse(success=True, message="Job triggered successfully.", build_number=build_number)

        return TriggerJobResponse(success=False, message="Failed to retrieve build number after retries.",
                                  build_number=None)
    except Exception as e:
        logger.error(f"Error triggering job {request.job_name}: {e}")
        return TriggerJobResponse(success=False, message=str(e), build_number=None)


@action_store.kubiya_action()
def get_build_console_logs(request: BuildConsoleLogsRequest) -> BuildConsoleLogsResponse:
    logger.info(f"Getting console logs for job {request.job_name}, build number {request.build_number}")
    job_name = request.job_name
    build_number = request.build_number
    console_logs_endpoint = f"/job/{job_name}/{build_number}/consoleText"
    console_logs = get_wrapper(console_logs_endpoint)

    return BuildConsoleLogsResponse(console_logs=console_logs)


@action_store.kubiya_action()
def get_artifact(request: ArtifactRequest):
    job_name = request.job_name
    build_number = request.build_number
    artifact_path = request.artifact_path
    artifact_endpoint = f"/job/{job_name}/{build_number}/artifact/{artifact_path}"
    artifact = get_wrapper(artifact_endpoint)

    return artifact