import json
import botocore
from boto3 import Session
from botocore.config import Config

from exceptions import ResponsePayloadException


class SampleLambdaCall:
    """
        Sample call to a fictional lambda function. This is implemented only to demonstrate the use of pytest
    """

    @staticmethod
    def get_lambda_client():
        return Session().client("lambda", config=Config(connect_timeout=50, read_timeout=185))

    @classmethod
    def execute_process(cls, text):

        try:
            response = cls.get_lambda_client().invoke(
                FunctionName="lambda_function_to_execute",
                InvocationType="RequestResponse",
                Payload=json.dumps({
                    "text": text
                }),
            )
            response_payload = response["Payload"].read()
            if "errorMessage" in response_payload:
                raise ResponsePayloadException(error_message=json.loads(response_payload)["errorMessage"])

            result_text = json.loads(response_payload)["result"]

            return {
                "success": True,
                "text": result_text,
                "error_message": None
            }
        except (
                botocore.vendored.requests.exceptions.ReadTimeout, ResponsePayloadException
        ) as err:
            return {
                "success": False,
                "error_message": str(err).strip('"')
            }


if __name__ == "__main__":
    lambda_call = SampleLambdaCall()
    lambda_call.execute_process("text_to_be_processed")
