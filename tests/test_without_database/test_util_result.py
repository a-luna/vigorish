from vigorish.util.result import Result


def show_value(value=None):
    print(f"Success! Result: {value}")
    return Result.Ok(value)


def show_error(error):
    print(f"Error: {error}")
    return Result.Fail(error)


def show_result(result):
    print(result)
    return result


def test_successful_result(capfd):
    result = Result.Ok(22).on_success(show_value)
    out, err = capfd.readouterr()
    assert "Success! Result: 22" in out
    assert not err
    assert result.success
    assert result.value == 22

    result = Result.Ok(22).on_failure(show_error)
    assert result.success
    assert result.value == 22

    result = Result.Ok(22).on_success(show_value).on_failure(show_error)
    out, err = capfd.readouterr()
    assert "Success! Result: 22" in out
    assert not err
    assert result.success
    assert result.value == 22

    result = Result.Ok(22).on_failure(show_error).on_success(show_value)
    out, err = capfd.readouterr()
    assert "Success! Result: 22" in out
    assert not err
    assert result.success
    assert result.value == 22

    result = Result.Ok().on_success(show_value)
    out, err = capfd.readouterr()
    assert "Success! Result: " in out
    assert not err
    assert result.success
    assert not result.value

    result = Result.Ok().on_failure(show_error)
    assert result.success
    assert not result.value

    result = Result.Ok().on_success(show_value).on_failure(show_error)
    out, err = capfd.readouterr()
    assert "Success! Result: " in out
    assert not err
    assert result.success
    assert not result.value

    result = Result.Ok().on_failure(show_error).on_success(show_value)
    assert "Success! Result: " in out
    assert not err
    assert result.success
    assert not result.value


def test_failed_result(capfd):
    result = Result.Fail("error!").on_failure(show_error)
    out, err = capfd.readouterr()
    assert "Error: error!" in out
    assert not err
    assert result.failure
    assert result.error == "error!"

    result = Result.Fail("error!").on_success(show_value)
    out, err = capfd.readouterr()
    assert not out
    assert not err
    assert result.failure
    assert result.error == "error!"

    result = Result.Fail("error!").on_success(show_value).on_failure(show_error)
    out, err = capfd.readouterr()
    assert "Error: error!" in out
    assert not err
    assert result.failure
    assert result.error == "error!"

    result = Result.Fail("error!").on_failure(show_error).on_success(show_value)
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

    result = Result.Fail("").on_success(show_value)
    out, err = capfd.readouterr()
    assert not out
    assert not err
    assert result.failure
    assert not result.error

    result = Result.Fail("").on_success(show_value).on_failure(show_error)
    out, err = capfd.readouterr()
    assert "Error: " in out
    assert not err
    assert result.failure
    assert not result.error

    result = Result.Fail("").on_failure(show_error).on_success(show_value)
    out, err = capfd.readouterr()
    assert "Error: " in out
    assert not err
    assert result.failure
    assert not result.error


def test_result_on_both(capfd):
    result = Result.Ok().on_both(show_result)
    out, err = capfd.readouterr()
    assert "[Success]" in out
    assert not err
    assert result.success
    assert not result.value

    result = Result.Ok(22).on_both(show_result)
    out, err = capfd.readouterr()
    assert "[Success] value=22" in out
    assert not err
    assert result.success
    assert result.value == 22

    result = Result.Fail("error!").on_both(show_result)
    out, err = capfd.readouterr()
    assert "[Fail] error!" in out
    assert not err
    assert result.failure
    assert result.error == "error!"


def test_result_combine():
    result1 = Result.Fail("error!")
    result2 = Result.Fail("another error!")
    combined_result = Result.Combine([result1, result2])
    assert combined_result.failure
    assert combined_result.error == "error!\nanother error!"