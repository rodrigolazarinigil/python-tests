import pytest
from unittest import mock

from pytest_parameterized_sample import SampleLambdaCall


@pytest.mark.parametrize(
    "p_input_url, p_lambda_client_return, p_expected_result",
    [
        # Case: Return json without error
        pytest.param(
            "fake text",
            '{"text": "fake text", "result": "Finished"}',
            {
                "success": True,
                "text": "Finished",
                "error_message": None,
            }
        ),
        # Case: Return json with error
        pytest.param(
            "fake text 2", '{"text": "", "errorMessage": "No texts found!"}', {
                "success": False,
                "error_message": "No texts found!"
            }
        )
    ]
)
def test_execute_process(mocker, p_input_url, p_lambda_client_return, p_expected_result):
    # Creating mocks
    mock_lambda_client = mock.MagicMock()
    mock_payload = mock.MagicMock()
    mock_response = {
        "Payload": mock_payload
    }

    # Patching lambda client
    mocker.patch("pytest_parameterized_sample.SampleLambdaCall.get_lambda_client", mock_lambda_client)

    # Define return values using the parameters set
    mock_payload.read.return_value = p_lambda_client_return
    mock_lambda_client.return_value.invoke.return_value = mock_response

    # Call function
    result = SampleLambdaCall.execute_process(p_input_url)

    # Assertions
    assert p_expected_result == result
