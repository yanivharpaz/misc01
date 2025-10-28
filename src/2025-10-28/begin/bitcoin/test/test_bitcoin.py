import block

def test_gb():
    node = block.Node('matt')
    txns = [] #1 -setup
    res = node.process_txns(txns)  #2
    gb, hash = res
    assert hash.startswith('0')  # 3
    
    
