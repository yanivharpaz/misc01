import skilift 

import pytest


@pytest.mark.bench_size(6)
def test_bench6(line5, BenchN):
    #line = skilift.Line(5)
    lift = skilift.Lift(10, BenchN)
    res = lift.one_bench(line5)
    assert res == {'loaded': 5, 'num_benches': 1, 'unloaded': 0}


@pytest.mark.line_size(6)
def test_line6(line_n):
    #line = skilift.Line(5)
    res = line_n.take(7)
    assert res == 6
    assert line_n.num_people == 0

def test_line_take(line5):
    #line = skilift.Line(5)
    res = line5.take(7)
    assert res == 5
    assert line5.num_people == 0

def test_lift_one_bench(line5, quad10):
    #line = skilift.Line(5)
    #lift = skilift.Lift(10, skilift.Quad)
    res = quad10.one_bench(line5)
    assert res == {'loaded': 4, 'num_benches': 1, 'unloaded': 0}

@pytest.mark.parametrize('size',
                         ['10', [], None])
def test_line_bad(size):
    line = skilift.Line(size)
    with pytest.raises(TypeError):
        res = line.take(7)


@pytest.mark.parametrize('line_size, take_num, result, remaining',
    [(0,5,0,0), (10,0,0,10), (5,2,2,3)])
def test_line_sizes(line_size, take_num, result, remaining):
    line = skilift.Line(line_size)
    res = line.take(take_num)
    assert res == result
    assert line.num_people == remaining

def test_half_take(monkeypatch, line5):
    def take(self, amount):
        amount = int(amount/2)  # change
        if amount > self.num_people:
            amount = self.num_people
        self.num_people -= amount
        return amount
    monkeypatch.setattr(skilift.Line, 'take', take)
    res = line5.take(5)
    assert res == 2
    
