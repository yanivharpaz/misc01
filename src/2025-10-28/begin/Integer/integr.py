def parse(txt):
    """Returns a list of numbers
    >>> parse('1,3')
    [1, 3]
    """
    res = []
    for thing in txt.split(','):
        res.append(int(thing))
    return res
