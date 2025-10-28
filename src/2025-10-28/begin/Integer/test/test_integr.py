import integr

import pytest

def test_basic():
    res = integr.parse('1,3,5,9')  #2
    name = 'matt'
    assert res == [1,3,5,9] , 'should be a list of integers!' #3

@pytest.mark.wrong
def test_bad1():
    with pytest.raises(ValueError):
        integr.parse('bad input')

@pytest.mark.wrong
@pytest.mark.xfail(raises=ValueError)
def test_bad2():
    res = integr.parse('1-3,12-15')
