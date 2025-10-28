import skilift 

import pytest

@pytest.fixture
def BenchN(request):
    size_ = request.node.get_closest_marker('bench_size').args[0]
    class Bench(skilift._Bench):
        size = size_
    return Bench

@pytest.fixture
def line_n(request):
    size = request.node.get_closest_marker('line_size').args[0]
    return skilift.Line(size)

@pytest.fixture
def line5():
    return skilift.Line(5)
    
@pytest.fixture
def quad10():
    return skilift.Lift(10, skilift.Quad)
