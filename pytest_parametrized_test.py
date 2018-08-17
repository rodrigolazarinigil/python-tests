import json

from app.worker import Worker
import pytest
from unittest import mock


@pytest.mark.parametrize(
    "p_exec_id, p_tries, p_url, p_execute_side_effect, p_expected_result, p_expected_s3_key, p_expected_log_list", [
        # Test case: Success on the first try
        pytest.param(
            1, 5, "fakeurl1", [
                {
                    "success": True,
                    "s3_key": "xyz.json",
                    "error_message": None
                }
            ], True, "xyz.json", [None]
        ),
        # Test case: Success on the third try
        pytest.param(
            1, 5, "https://stackoverflow.com/", [
                {
                    "success": False,
                    "s3_key": "",
                    "error_message": "Timeout"
                },
                {
                    "success": False,
                    "s3_key": "",
                    "error_message": "Timeout"
                },
                {
                    "success": True,
                    "s3_key": "wcwc.json",
                    "error_message": None
                }
            ], True, "wcwc.json", ["Timeout", "Timeout"]
        ),
        # Test case: Failure after two tries
        pytest.param(
            1, 2, "&@&#UJC", [
                {
                    "success": False,
                    "s3_key": "",
                    "error_message": "Timeout"
                },
                {
                    "success": False,
                    "s3_key": "",
                    "error_message": "Weird Error"
                }
            ], False, "", ["Timeout", "Weird Error"]
        )
    ]
)
def test_study_execution(
    mocker, p_exec_id, p_tries, p_url, p_execute_side_effect, p_expected_result, p_expected_s3_key, p_expected_log_list
):
    """
        Checks if the worker:
        - Updates the job status before executing;
        - Executes the correct URL;
        - Call the execution multiple times, until it works
        - The ammount of execution calls;
        - If the correct result is saved to the database.
        
    :param mocker: Pytest mocker object
    :param p_exec_id: Test parametrized execution id
    :param p_tries: Test parametrized number os tries
    :param p_execute_side_effect: Test parametrized side effects for the 'execute_url' calls
    :param p_expected_result: Test parametrized expected result True | False
    :param p_expected_s3_key: Test parametrized expected S3 key saved to the database
    :param p_expected_log_list: Test parametrized expected log list messages
    :return:
    """
    # Mock StudyExecutionWrapper
    mock_study_execution = mock.MagicMock()
    mock_study_execution.return_value.get_max_retries.return_value = p_tries
    mock_study_execution.return_value.get_url.return_value = p_url
    mocker.patch("app.worker.StudyExecutionWrapper", mock_study_execution)
    
    # Mock execute url
    mock_execute_url = mock.MagicMock()
    mock_execute_url.side_effect = p_execute_side_effect
    mocker.patch("app.worker.Worker.execute_url", mock_execute_url)
    
    # Call to test
    worker = Worker()
    worker.study_execute(execution_id=p_exec_id)
    
    # Assertions
    mock_study_execution.assert_called_once_with(study_execution_id=p_exec_id)
    mock_study_execution.return_value.set_status_as_running.assert_called_once_with()
    mock_execute_url.assert_called_with(p_url)
    log_calls = []
    for message in p_expected_log_list:
        log_calls.append(mock.call(message=message))
    mock_study_execution.return_value.log_error_message.assert_has_calls(log_calls)
    assert len(p_execute_side_effect) == mock_study_execution.return_value.log_error_message.call_count
    mock_study_execution.return_value.save_result.assert_called_once_with(
        s3_key=p_expected_s3_key,
        success=p_expected_result)


@pytest.mark.parametrize("p_url, p_error_message", [
    # Test case: Successful call
    pytest.param("https://stackoverflow.com/", None),
    # Test case: Failure (has the error_message on response)
    pytest.param("https://pypi.org/project/pytest-mock/", "Fake error message")
])
def test_execute_url(mocker, p_url, p_error_message):
    """
        Checks the 'Worker.execute_url' method:
        
    :param mocker: Pytest mocker object
    :param p_url: Test parametrized URL
    :param p_error_message: Parametrized error message
    """
    # Mock response
    mock_response = mock.MagicMock()
    if p_error_message is None:
        mock_response.read.return_value = '{"s3Key": "kcmanvioansk.json"}'
    else:
        mock_response.read.return_value = '{{"s3Key": "kcmanvioansk.sjon","errorMessage": "{message}"}}'.format(
            message=p_error_message)
    
    # Mock lambda client
    mock_lambda_client = mock.MagicMock()
    mock_lambda_client.return_value.invoke.return_value = {
        "Payload": mock_response
    }
    mocker.patch("app.worker.Worker.get_lambda_client", mock_lambda_client)
    
    # Call to test
    worker = Worker()
    result = worker.execute_url(p_url)
    
    # Assertions
    if p_error_message is None:
        assert {
                   "success": True,
                   "s3_key": "kcmanvioansk.json",
                   "error_message": None
               } == result
    else:
        assert {
                   "success": False,
                   "s3_key": None,
                   "error_message": p_error_message
               } == result
    mock_lambda_client.return_value.invoke.assert_called_once_with(
        FunctionName="lighthouse-crawler-stage",
        InvocationType="RequestResponse",
        Payload=json.dumps({
            "url": p_url
        })
    )
