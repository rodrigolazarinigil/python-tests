import json
import botocore
from boto3 import Session
from botocore.config import Config

from app.config import config
from app.db.wrapper import StudyExecutionWrapper
from app.exceptions import ResponsePayloadException


class Worker:
    """
        Contains the functions to execute studies and save the results on S3 and the database.
    """
    
    @staticmethod
    def get_lambda_client():
        """
            Returns a lambda client object
        """
        return Session().client("lambda", config=Config(connect_timeout=50, read_timeout=185))
    
    def study_execute(self, execution_id):
        """
            Get data from one study execution, calls lambda function and saves the result
        :param execution_id: Id from the study_execution table
        """
        db_wrapper = StudyExecutionWrapper(study_execution_id=execution_id)
        db_wrapper.set_status_as_running()
        
        retry_no = 1
        success = False
        result = {
            "success": None,
            "s3_key": None,
            "error_message": None
        }
        
        while not success and retry_no <= db_wrapper.get_max_retries():
            result = self.execute_url(db_wrapper.get_url())
            
            success = result["success"]
            db_wrapper.log_error_message(message=result["error_message"])
            retry_no += 1
        
        db_wrapper.save_result(
            success=result["success"],
            s3_key=result["s3_key"]
        )
    
    @classmethod
    def execute_url(cls, url):
        """
            Call a lambda function passing a URL as parameter
        :param url: URL received from the REDIS queues
        """
        payload = {
            "url": url
        }

        try:
            response = cls.get_lambda_client().invoke(
                FunctionName=config.LAMBDA_FUNCTION_NAME,
                InvocationType="RequestResponse",
                Payload=json.dumps(payload),
            )
            response_payload = response["Payload"].read()
            if "errorMessage" in response_payload:
                raise ResponsePayloadException(error_message=json.loads(response_payload)["errorMessage"])
            
            s3_key = json.loads(response_payload)["s3Key"]
            
            return {
                "success": True,
                "s3_key": s3_key,
                "error_message": None
            }
        except (
            botocore.vendored.requests.exceptions.ReadTimeout, KeyError, TypeError, ResponsePayloadException
        ) as err:
            return {
                "success": False,
                "s3_key": None,
                "error_message": str(err).strip('"')
            }


def study_execute(study_execution_id):
    """
        Procedure to execute an URL
    :param study_execution_id:
    :return: JSON with an s3 key representing the file saved in case of success or an error message in case of failure
    """
    worker = Worker()
    return worker.study_execute(study_execution_id)


if __name__ == "__main__":
    worker = Worker()
    worker.execute_url("https://inbox.google.com/")
