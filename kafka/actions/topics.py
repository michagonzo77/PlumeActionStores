from pydantic import BaseModel, Field
from . import action_store as action_store
from .jobs import *
import logging


logger = logging.getLogger(__name__)


class DescribeKafkaTopicRequest(BaseModel):
    """
    Request to describe a Kafka topic by providing its exact name for a full match.
    If you're not sure of the exact name, you can use the 'list topics' action
    that accepts partial matches to retrieve a list of matching topics.
    """
    topic_name: str = Field(
        ...,
        description="The exact name of the Kafka topic to describe. Use the full name for a match. If unsure, "
                    "you can use the 'list topics' action for partial matches."
    )

class DescribeKafkaTopicResponse(BaseModel):
    artifact: str
    success: bool

@action_store.kubiya_action()
def describe_kafka_topic(request: DescribeKafkaTopicRequest) -> DescribeKafkaTopicResponse:
    """
    Describe Kafka topic.

    Args:
        request (DescribeKafkaTopicRequest): The request containing the topic name to return Kafka topic description.

    Returns:
        DescribeKafkaTopicResponse: The response containing the description of the Kafka topic.

    Examples:
        - To describe a specific Kafka topic, provide a full match of the topic name:
          ```
          request = DescribeKafkaTopicRequest(topic_name="your_topic_name")
          response = describe_kafka_topic(request)
          ```

        - To get the number of partitions for a specific Kafka topic:
          ```
          request = DescribeKafkaTopicRequest(topic_name="your_topic_name")
          response = describe_kafka_topic(request)
          ```
        
        - To get the replication factor for a specific Kafka topic:
          ```
          request = DescribeKafkaTopicRequest(topic_name="your_topic_name")
          response = describe_kafka_topic(request)
          ```

        - To retrieve the Kafka topic configuration for a specific Kafka topic:
          ```
          request = DescribeKafkaTopicRequest(topic_name="your_topic_name")
          response = describe_kafka_topic(request)
          ```

        - To obtain the leader and replicas information for a specific Kafka topic:
          ```
          request = DescribeKafkaTopicRequest(topic_name="your_topic_name")
          response = describe_kafka_topic(request)
          ```

    """
    try:
        logger.info(f"Triggering job kafka_describe_topic")

        try:
            trigger_describe_job_request = TriggerJobRequest(job_name="kafka_describe_topic", parameters=f"?topic_name={request.topic_name}")
            describe_topic_job = trigger_job(trigger_describe_job_request)
        except Exception as e:
            return DescribeKafkaTopicResponse(artifact=f"Error triggering describe job: {e}", success=False)

        try:
            wait_for_describe_job_completion_request = BuildParams(job_name="kafka_describe_topic", build_number=describe_topic_job.build_number)
            wait_for_describe_job_completion_response = wait_for_job_completion(wait_for_describe_job_completion_request)
        except Exception as e:
            return DescribeKafkaTopicResponse(artifact=f"Error waiting for describe job completion: {e}", success=False)

        if wait_for_describe_job_completion_response.status != "SUCCESS":
            return DescribeKafkaTopicResponse(artifact=f"Jenkins job failed: Possibly no match for your topic. {wait_for_describe_job_completion_response}", success=False)

        try:
            get_describe_artifact_request = ArtifactRequest(job_name="kafka_describe_topic", build_number=describe_topic_job.build_number, artifact_path="output.txt")
            get_describe_artifact_response = get_artifact(get_describe_artifact_request)
        except Exception as e:
            return DescribeKafkaTopicResponse(artifact=f"Error getting describe artifact: {e}", success=False)
        
        if get_describe_artifact_response:
            return DescribeKafkaTopicResponse(artifact=get_describe_artifact_response, success=True)
        else:
            return DescribeKafkaTopicResponse(artifact="No artifact response found.", success=False)

    except Exception as e:
        logger.error(f"Error describing Kafka topic: {e}")
        return DescribeKafkaTopicResponse(artifact=f"Error with entire try section: {e}", success=False)


class ListKafkaTopicsRequest(BaseModel):
    """
    Request object for listing Kafka topics.

    Args:
        topic_name (str): The name of the topic to filter by. Use '.' to match all topics
            or provide a partial match to get a list of matching topics.
    """
    topic_name: str = "."

class ListKafkaTopicsResponse(BaseModel):
    topics: str
    success: bool


@action_store.kubiya_action()
def list_kafka_topics(request: ListKafkaTopicsRequest) -> ListKafkaTopicsResponse:
    """
    Lists Kafka topics.

    Args:
        request (ListKafkaTopicsRequest): The request containing a partial match of the topic name or * to return all topics.

    Returns:
        ListKafkaTopicsResponse: The response containing the list of topics.
    """
    try:
        logger.info(f"Triggering job kafka_list_topics")

        try:
            trigger_job_request = TriggerJobRequest(job_name="kafka_list_topics", parameters=f"?regex={request.topic_name}")
            topic_job = trigger_job(trigger_job_request)
        except Exception as e:
            return ListKafkaTopicsResponse(topics=f"Error triggering job: {e}", success=False)
        
        try:
            wait_for_job_completion_request = BuildParams(job_name="kafka_list_topics", build_number=topic_job.build_number)
            wait_for_job_completion_response = wait_for_job_completion(wait_for_job_completion_request)
        except Exception as e:
            return ListKafkaTopicsResponse(topics=f"Error waiting for job completion: {e}", success=False)

        if wait_for_job_completion_response.status != "SUCCESS":
            return ListKafkaTopicsResponse(topics=f"Jenkins job failed. No matching topics found", success=False)
        
        try:
            get_artifact_request = ArtifactRequest(job_name="kafka_list_topics", build_number=topic_job.build_number, artifact_path="output.txt")
            get_artifact_response = get_artifact(get_artifact_request)
        except Exception as e:
            return ListKafkaTopicsResponse(topics=f"Error getting describe artifact: {e}", success=False)
            
        if get_artifact_response:
            return ListKafkaTopicsResponse(topics=get_artifact_response, success=True)
        else:
            return ListKafkaTopicsResponse(topics="No artifact response found.", success=False)
        
    except Exception as e:
        logger.error(f"Error listing Kafka topics: {e}")
        return ListKafkaTopicsResponse(topics=f"Error with entire try section: {e}", success=False)
    
@action_store.kubiya_action()
def number_of_partitions_in_kafka_topic(request: DescribeKafkaTopicRequest) -> DescribeKafkaTopicResponse:
    request = DescribeKafkaTopicRequest(topic_name=request.topic_name)
    return describe_kafka_topic(request)

@action_store.kubiya_action()
def replication_factor_of_kafka_topic(request: DescribeKafkaTopicRequest) -> DescribeKafkaTopicResponse:
    request = DescribeKafkaTopicRequest(topic_name=request.topic_name)
    return describe_kafka_topic(request)

@action_store.kubiya_action()
def leader_and_replicas_for_kafka_topic(request: DescribeKafkaTopicRequest) -> DescribeKafkaTopicResponse:
    request = DescribeKafkaTopicRequest(topic_name=request.topic_name)
    return describe_kafka_topic(request)


# def remove_escape_sequences(text):
#     # Remove ANSI escape sequences
#     escape_sequence_pattern = re.compile(r'\x1b[^m]*m')
#     return escape_sequence_pattern.sub('', text)


# Original parse artifact response function -- does not remove the garbage data from Kafka
# @action_store.kubiya_action()
# def parse_artifact_response(artifact_response):
#     # Remove escape sequences from the input text
#     cleaned_response = remove_escape_sequences(artifact_response)

#     # Split the cleaned input text into lines
#     lines = cleaned_response.strip().split('\n')

#     # Extract column headers from the first row
#     headers = lines[0].split()

#     # Initialize an empty list to store lists for each row
#     result = []

#     # Iterate through the rest of the rows and create lists
#     for line in lines[1:]:
#         values = line.split()

#         # Ensure the number of values matches the number of headers
#         if len(values) < len(headers):
#             values += [''] * (len(headers) - len(values))

#         result.append(values)

#     # Use tabulate to format the data as a table
#     table = tabulate(result, headers=headers, tablefmt="grid")

#     # Wrap the table with Slack's code block style ("```")
#     slack_code_block = "```" + table + "```"

#     return slack_code_block



### Remove the garbage data from Kafka ###
# @action_store.kubiya_action()
# def parse_artifact_response(artifact_response):
#     # Remove escape sequences from the input text
#     cleaned_response = remove_escape_sequences(artifact_response)

#     # Split the cleaned input text into lines
#     lines = cleaned_response.strip().split('\n')

#     # Find the index where the "Topic:" line appears
#     topic_index = None
#     for i, line in enumerate(lines):
#         if line.startswith("Topic:"):
#             topic_index = i
#             break

#     if topic_index is None:
#         # "Topic:" line not found, return an error message
#         return "Topic information not found in the artifact."

#     # Extract lines from the "Topic:" section to the end
#     topic_lines = lines[topic_index:]

#     # Extract column headers from the first row
#     headers = topic_lines[0].split()

#     # Initialize an empty list to store lists for each row
#     result = []

#     # Iterate through the rest of the rows and create lists
#     for line in topic_lines[1:]:
#         values = line.split()

#         # Ensure the number of values matches the number of headers
#         if len(values) < len(headers):
#             values += [''] * (len(headers) - len(values))

#         result.append(values)

#     # Use tabulate to format the data as a table
#     table = tabulate(result, headers=headers, tablefmt="grid")

#     # Wrap the table with Slack's code block style ("```")
#     slack_code_block = "```" + table + "```"

#     return slack_code_block


# Includes a step which attempts to parse and beautify the output of the Jenkins job
# @action_store.kubiya_action()
# def describe_kafka_topic(request: DescribeKafkaTopicRequest) -> DescribeKafkaTopicResponse:
#     """
#     Describe Kafka topic.

#     Args:
#         request (DescribeKafkaTopicRequest): The request containing the topic name to return Kafka topic description.

#     Returns:
#         DescribeKafkaTopicResponse: The response containing the description of the Kafka topic.

#     Examples:
#         - To describe a specific Kafka topic, provide a full match of the topic name:
#           ```
#           request = DescribeKafkaTopicRequest(topic_name="your_topic_name")
#           response = describe_kafka_topic(request)
#           ```

#         - To get the number of partitions for a specific Kafka topic:
#           ```
#           request = DescribeKafkaTopicRequest(topic_name="your_topic_name")
#           response = describe_kafka_topic(request)
#           ```

#     """
#     try:
#         logger.info(f"Triggering job kafka_describe_topic")

#         try:
#             trigger_describe_job_request = TriggerJobRequest(job_name="kafka_describe_topic", parameters=f"?topic_name={request.topic_name}")
#             describe_topic_job = trigger_job(trigger_describe_job_request)
#         except Exception as e:
#             return DescribeKafkaTopicResponse(artifact=f"Error triggering describe job: {e}", success=False)

#         try:
#             wait_for_describe_job_completion_request = BuildParams(job_name="kafka_describe_topic", build_number=describe_topic_job.build_number)
#             wait_for_describe_job_completion_response = wait_for_job_completion(wait_for_describe_job_completion_request)
#         except Exception as e:
#             return DescribeKafkaTopicResponse(artifact=f"Error waiting for describe job completion: {e}", success=False)

#         if wait_for_describe_job_completion_response.status != "SUCCESS":
#             return DescribeKafkaTopicResponse(artifact=f"Jenkins job failed: Possibly no match for your topic. {wait_for_describe_job_completion_response}", success=False)

#         try:
#             get_describe_artifact_request = ArtifactRequest(job_name="kafka_describe_topic", build_number=describe_topic_job.build_number, artifact_path="output.txt")
#             get_describe_artifact_response = get_artifact(get_describe_artifact_request)
#         except Exception as e:
#             return DescribeKafkaTopicResponse(artifact=f"Error getting describe artifact: {e}", success=False)

#         if get_describe_artifact_response:
#             topic_description = parse_artifact_response(get_describe_artifact_response)
#             return DescribeKafkaTopicResponse(artifact=topic_description, success=True)
#         else:
#             return DescribeKafkaTopicResponse(artifact="No artifact response found.", success=False)

#     except Exception as e:
#         logger.error(f"Error describing Kafka topic: {e}")
#         return DescribeKafkaTopicResponse(artifact=f"Error with entire try section: {e}", success=False)