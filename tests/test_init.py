import datamine


def test_package_has_version():
    assert hasattr(datamine, '__version__')
    assert len(datamine.__version__) > 0