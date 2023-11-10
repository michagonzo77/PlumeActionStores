from pydantic import BaseModel
from . import action_store as action_store
from .jobs import *
from typing import List
import logging


logger = logging.getLogger(__name__)


class DescribeKafkaConsumerGroupRequest(BaseModel):
    consumer_group_name: str

class DescribeKafkaConsumerGroupResponse(BaseModel):
    artifact: str
    success: bool


@action_store.kubiya_action()
def describe_kafka_consumer_group(request: DescribeKafkaConsumerGroupRequest) -> DescribeKafkaConsumerGroupResponse:
    """
    Describe Kafka consumer group.

    Args:
        request (DescribeKafkaConsumerGroupRequest): The request containing the consumer group name to return Kafka consumer group description.

    Returns:
        DescribeKafkaConsumerGroupResponse: The response containing the description of the Kafka consumer group.

    Examples:
        - To describe a specific Kafka consumer group, provide a full match of the consumer group name:
          ```
          request = DescribeKafkaConsumerGroupRequest(consumer_group_name="your_consumer_group_name")
          response = describe_kafka_consumer_group(request)
          ```

        - To get the number of partitions for a specific Kafka consumer group:
          ```
          request = DescribeKafkaConsumerGroupRequest(consumer_group_name="your_consumer_group_name")
          response = describe_kafka_consumer_group(request)
          ```

        - To retrieve the current offset, log-end-offset, and lag for a specific Kafka consumer group:
          ```
          request = DescribeKafkaConsumerGroupRequest(consumer_group_name="your_consumer_group_name")
          response = describe_kafka_consumer_group(request)
          ```

        - To get the consumer ID and host information for a specific Kafka consumer group:
          ```
          request = DescribeKafkaConsumerGroupRequest(consumer_group_name="your_consumer_group_name")
          response = describe_kafka_consumer_group(request)
          ```

        - To obtain the partition-specific details, such as current offset, log-end-offset, and lag, for a specific Kafka consumer group:
          ```
          request = DescribeKafkaConsumerGroupRequest(consumer_group_name="your_consumer_group_name")
          response = describe_kafka_consumer_group(request)
          ```

        - To check the client ID for a specifc Kafka consumer group:
          ```
          request = DescribeKafkaConsumerGroupRequest(consumer_group_name="your_consumer_group_name")
          response = describe_kafka_consumer_group(request)
          ```
    """
    try:
        logger.info(f"Triggering job kafka_describe_consumer_group")

        try:
            trigger_describe_job_request = TriggerJobRequest(job_name="kafka_describe_consumer_group", parameters=f"?group_name={request.consumer_group_name}")
            describe_consumer_group_job = trigger_job(trigger_describe_job_request)
        except Exception as e:
            return DescribeKafkaConsumerGroupResponse(artifact=f"Error triggering describe job: {e}", success=False)
        
        try:
            wait_for_describe_job_completion_request = BuildParams(job_name="kafka_describe_consumer_group", build_number=describe_consumer_group_job.build_number)
            wait_for_describe_job_completion_response = wait_for_job_completion(wait_for_describe_job_completion_request)
        except:
            return DescribeKafkaConsumerGroupResponse(artifact=f"Error waiting for describe job completion: {e}", success=False)

        if wait_for_describe_job_completion_response.status != "SUCCESS":
            return DescribeKafkaConsumerGroupResponse(artifact=f"Jenkins job failed: {wait_for_describe_job_completion_response}", success=False)
        
        try:
            get_describe_artifact_request = ArtifactRequest(job_name="kafka_describe_consumer_group", build_number=describe_consumer_group_job.build_number, artifact_path="output.txt")
            get_describe_artifact_response = get_artifact(get_describe_artifact_request)
        except Exception as e:
            return DescribeKafkaConsumerGroupResponse(artifact=f"Error getting describe artifact: {e}", success=False)

        if get_describe_artifact_response:
            return DescribeKafkaConsumerGroupResponse(artifact=get_describe_artifact_response, success=True)
        else:
            return DescribeKafkaConsumerGroupResponse(artifact="No artifact response found.", success=False)
        
    except Exception as e:
        logger.error(f"Error describing Kafka consumer group: {e}")
        return DescribeKafkaConsumerGroupResponse(artifact=f"Error with entire try section: {e}", success=False)


class ListKafkaConsumerGroupsRequest(BaseModel):
    """
    Request object for listing Kafka consumer groups.

    Args:
        consumer_group (str): The name of the consumer group to filter by. Use '.' to match all consumer groups
            or provide a partial match to get a list of matching consumer groups.
    """
    consumer_group_name: str = "."

class ListKafkaConsumerGroupsResponse(BaseModel):
    consumer_groups: List[str]
    success: bool


def split_kafka_output(output: str) -> List[str]:
    """
    Splits Kafka output by lines (separator: \r\n).

    Args:
        output (str): The Kafka output string.

    Returns:
        List[str]: A list of lines from the Kafka output.
    """
    # Split the output by the \r\n separator
    lines = output.split('\r\n')
    
    return lines


@action_store.kubiya_action()
def list_kafka_consumer_groups(request: ListKafkaConsumerGroupsRequest) -> ListKafkaConsumerGroupsResponse:
    """
    Lists Kafka consumer groups.

    Args:
        request (ListKafkaConsumerGroupsRequest): The request containing a partial match of the consumer group name or * to return all consumer groups.

    Returns:
        ListKafkaConsumerGroupsResponse: The response containing the list of consumer groups.

    Examples:
        - List consumer groups matching a partial name:
          ```
          request = ListKafkaConsumerGroupsRequest(consumer_group_name="your_consumer_group_name")
          response = list_kafka_consumer_groups(request)
          ```
    """
    try:
        logger.info(f"Triggering job kafka_list_consumer_groups")

        try:
            trigger_job_request = TriggerJobRequest(job_name="kafka_list_consumer_groups", parameters=f"?regex={request.consumer_group_name}")
            consumer_group_job = trigger_job(trigger_job_request)
        except Exception as e:
            return ListKafkaConsumerGroupsResponse(consumer_groups=f"Error triggering describe job: {e}", success=False)
        
        try:
            wait_for_job_completion_request = BuildParams(job_name="kafka_list_consumer_groups", build_number=consumer_group_job.build_number)
            wait_for_job_completion_response = wait_for_job_completion(wait_for_job_completion_request)
        except:
            return ListKafkaConsumerGroupsResponse(consumer_groups=f"Error waiting for job completion: {e}", success=False)
        
        if wait_for_job_completion_response.status != "SUCCESS":
            return ListKafkaConsumerGroupsResponse(consumer_groups=f"Jenkins job failed: {wait_for_job_completion_response}", success=False)
        
        try:
            get_artifact_request = ArtifactRequest(job_name="kafka_list_consumer_groups", build_number=consumer_group_job.build_number, artifact_path="output.txt")
            get_artifact_response = get_artifact(get_artifact_request)
        except Exception as e:
            return ListKafkaConsumerGroupsResponse(consumer_groups=f"Error getting describe artifact: {e}", success=False)

        if get_artifact_response:
            consumer_groups = split_kafka_output(get_artifact_response)
            return ListKafkaConsumerGroupsResponse(consumer_groups=consumer_groups, success=True)
        else: 
            return ListKafkaConsumerGroupsResponse(consumer_groups="No artifact response found.", success=False)
        
    except Exception as e:
        logger.error(f"Error listing Kafka consumer groups: {e}")
        return ListKafkaConsumerGroupsResponse(consumer_groups="Error with entire try section", success=False)



class GetLagOfKafkaConsumerGroupRequest(BaseModel):
    consumer_group_name: str

class TopicLag(BaseModel):
    topic: str
    lag: int

class GetLagOfKafkaConsumerGroupResponse(BaseModel):
    lag: List[TopicLag]
    success: str


def clean_up_response(response):
    # Find the index of "GROUP" in the response
    group_index = response.find("GROUP")
    
    # If "GROUP" is found, return the substring starting from that index
    if group_index != -1:
        cleaned_response = response[group_index:]
        return cleaned_response
    else:
        # If "GROUP" is not found, return the original response
        return response
    
def extract_lag_values(cleaned_response: str) -> List[dict]:
    lag_values = []
    try:
        # Split the cleaned response by lines
        lines = cleaned_response.strip().split('\r')

        for line in lines[1:]:  # Skip the header line
            columns = line.strip().split()
            if len(columns) >= 7:
                topic = columns[1]  # Topic is in the second column
                lag = int(columns[5])  # Lag is in the sixth column
                lag_values.append({"topic": topic, "lag": lag})

    except Exception as e:
        logger.error(f"Error extracting lag values: {e}")

    return lag_values

@action_store.kubiya_action()
def get_lag_of_kafka_consumer_group(request: GetLagOfKafkaConsumerGroupRequest) -> GetLagOfKafkaConsumerGroupResponse:
    """
    Get lag of a Kafka consumer group.

    Args:
        request (GetLagOfKafkaConsumerGroupRequest): The request containing the consumer group name to retrieve the lag.

    Returns:
        GetLagOfKafkaConsumerGroupResponse: The response containing the lag of the Kafka consumer group and other details.

    Examples:
        - To get the lag of a specific Kafka consumer group, provide a full match of the consumer group name:
          ```
          request = GetLagOfKafkaConsumerGroupRequest(consumer_group_name="your_consumer_group_name")
          response = get_lag_of_kafka_consumer_group(request)
          ```
    """
    try:
        logger.info(f"Triggering job kafka_get_lag_of_consumer_group")

        try:
            trigger_get_lag_job_request = TriggerJobRequest(job_name="kafka_describe_consumer_group", parameters=f"?group_name={request.consumer_group_name}")
            get_lag_consumer_group_job = trigger_job(trigger_get_lag_job_request)
        except Exception as e:
            return GetLagOfKafkaConsumerGroupResponse(lag=[], success=f"False: Error triggering get lag job: {e}")

        try:
            wait_for_get_lag_job_completion_request = BuildParams(job_name="kafka_describe_consumer_group", build_number=get_lag_consumer_group_job.build_number)
            wait_for_get_lag_job_completion_response = wait_for_job_completion(wait_for_get_lag_job_completion_request)
        except Exception as e:
            return GetLagOfKafkaConsumerGroupResponse(lag=[], success=f"False: Error waiting for get lag job completion: {e}")

        if wait_for_get_lag_job_completion_response.status != "SUCCESS":
            return GetLagOfKafkaConsumerGroupResponse(lag=[], success=f"False: Jenkins job failed: {wait_for_get_lag_job_completion_response}")

        try:
            get_lag_artifact_request = ArtifactRequest(job_name="kafka_describe_consumer_group", build_number=get_lag_consumer_group_job.build_number, artifact_path="output.txt")
            get_lag_artifact_response = get_artifact(get_lag_artifact_request)
        except Exception as e:
            return GetLagOfKafkaConsumerGroupResponse(lag=[], success=f"False: Error getting lag artifact: {e}")

        if get_lag_artifact_response:
            # Clean up the response
            cleaned_response = clean_up_response(get_lag_artifact_response)
            
            # Extract lag values for all partitions
            lag_values = extract_lag_values(cleaned_response)

            # Create a response with the lag information
            response = GetLagOfKafkaConsumerGroupResponse(lag=lag_values, success=f"True: Successfully retrieved lag of consumer group {request.consumer_group_name}")

            return response
        else:
            return GetLagOfKafkaConsumerGroupResponse(lag=[], success="False: No artifact response found.")

    except Exception as e:
        logger.error(f"Error getting lag of Kafka consumer group: {e}")
        return GetLagOfKafkaConsumerGroupResponse(lag=[], success=f"False: Error with entire try section: {e}")



# def remove_escape_sequences(text):
#     # Remove ANSI escape sequences
#     escape_sequence_pattern = re.compile(r'\x1b[^m]*m')
#     return escape_sequence_pattern.sub('', text)

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
#     table = tabulate(result, headers=headers, tablefmt="plain")

#     # Wrap the table with Slack's code block style ("```")
#     slack_code_block = "```" + table + "```"

#     return slack_code_block


# @action_store.kubiya_action()
# def describe_kafka_consumer_group(request: DescribeKafkaConsumerGroupRequest) -> DescribeKafkaConsumerGroupResponse:
#     """
#     Describe Kafka consumer group.

#     Args:
#         request (DescribeKafkaConsumerGroupRequest): The request containing the consumer group name to return Kafka consumer group description.

#     Returns:
#         DescribeKafkaConsumerGroupResponse: The response containing the description of the Kafka consumer group.

#     Examples:
#         - To describe a specific Kafka consumer group, provide a full match of the consumer group name:
#           ```
#           request = DescribeKafkaConsumerGroupRequest(consumer_group_name="your_consumer_group_name")
#           response = describe_kafka_consumer_group(request)
#           ```

#         - To get the number of partitions for a specific Kafka consumer group:
#           ```
#           request = DescribeKafkaConsumerGroupRequest(consumer_group_name="your_consumer_group_name")
#           response = describe_kafka_consumer_group(request)
#           ```
#     """
#     try:
#         logger.info(f"Triggering job kafka_describe_consumer_group")

#         try:
#             trigger_describe_job_request = TriggerJobRequest(job_name="kafka_describe_consumer_group", parameters=f"?group_name={request.consumer_group_name}")
#             describe_consumer_group_job = trigger_job(trigger_describe_job_request)
#         except Exception as e:
#             return DescribeKafkaConsumerGroupResponse(artifact=f"Error triggering describe job: {e}", success=False)
        
#         try:
#             wait_for_describe_job_completion_request = BuildParams(job_name="kafka_describe_consumer_group", build_number=describe_consumer_group_job.build_number)
#             wait_for_describe_job_completion_response = wait_for_job_completion(wait_for_describe_job_completion_request)
#         except:
#             return DescribeKafkaConsumerGroupResponse(artifact=f"Error waiting for describe job completion: {e}", success=False)

#         if wait_for_describe_job_completion_response.status != "SUCCESS":
#             return DescribeKafkaConsumerGroupResponse(artifact=f"Jenkins job failed: {wait_for_describe_job_completion_response}", success=False)
        
#         try:
#             get_describe_artifact_request = ArtifactRequest(job_name="kafka_describe_consumer_group", build_number=describe_consumer_group_job.build_number, artifact_path="output.txt")
#             get_describe_artifact_response = get_artifact(get_describe_artifact_request)
#         except Exception as e:
#             return DescribeKafkaConsumerGroupResponse(artifact=f"Error getting describe artifact: {e}", success=False)

#         if get_describe_artifact_response:
#             consumer_group = parse_artifact_response(get_describe_artifact_response)
#             return DescribeKafkaConsumerGroupResponse(artifact=consumer_group, success=True)
#         else:
#             return DescribeKafkaConsumerGroupResponse(artifact="No artifact response found.", success=False)
        
#     except Exception as e:
#         logger.error(f"Error describing Kafka consumer group: {e}")
#         return DescribeKafkaConsumerGroupResponse(artifact=f"Error with entire try section: {e}", success=False)