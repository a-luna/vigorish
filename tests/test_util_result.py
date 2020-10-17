from vigorish.util.result import Result


def result_success_no_return_value():
    return Result.Ok()


def result_success_return_value():
    return Result.Ok(22)


def result_failure():
    return Result.Fail("error!")


def show_value(value):
    print(f"Success! Result: {value}")
    return Result.Ok(value)


def show_error(error):
    print(f"Error: {error}")
    return Result.Fail(error)


def show_result(result):
    print(result)
    return result


def test_result_on_success(capfd):
    result = result_success_return_value().on_success(show_value)
    out, err = capfd.readouterr()
    assert "Success! Result: 22" in out
    assert not err
    assert result.success
    assert result.value == 22


def test_result_on_failure(capfd):
    result = result_failure().on_failure(show_error)
    out, err = capfd.readouterr()
    assert "Error: error!" in out
    assert not err
    assert result.failure
    assert result.error == "error!"

    result = Result.Fail("").on_failure(show_error)
    out, err = capfd.readouterr()
    assert "Error: " in out
    assert not err
    assert result.failure
    assert not result.error

    result = result_success_return_value().on_failure(show_error)
    assert result.success
    assert result.value == 22

    result = result_success_no_return_value().on_failure(show_error)
    assert result.success
    assert not result.value


def test_result_on_both(capfd):
    result = result_success_no_return_value().on_both(show_result)
    out, err = capfd.readouterr()
    assert "[Success]" in out
    assert not err
    assert result.success
    assert not result.value

    result = result_success_return_value().on_both(show_result)
    out, err = capfd.readouterr()
    assert "[Success]" in out
    assert not err
    assert result.success
    assert result.value == 22

    result = result_failure().on_both(show_result)
    out, err = capfd.readouterr()
    assert "[Failure] error!" in out
    assert not err
    assert result.failure
    assert result.error == "error!"


def test_result_combine():
    result1 = result_failure()
    result2 = result_failure()
    combined_result = Result.Combine([result1, result2])
    assert combined_result.failure
    assert combined_result.error == "error!\nerror!"
