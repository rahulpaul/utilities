# Given a binary tree, find out if it is a binary search tree or not.


class Node:

    def __init__(self, data, left=None, right=None):
        self.data = data
        self.left = left
        self.right = right
    
    def __str__(self):
        def _data(node):
            return node.data if node else None
        return f'({self.data}) -> ({_data(self.left)}, {_data(self.right)})'
    

def verify_bst(node: Node):
    return _verify_bst_internal(node, None, None)


def _verify_bst_internal(node: Node, t_min, t_max):
    if node is None:
        return True
    
    if t_min is not None and t_min > node.data:
        return False
    if t_max is not None and t_max <= node.data:
        return False

    # traverse left 
    if not _verify_bst_internal(node.left, t_min=t_min, t_max=node.data):
        return False
    # traverse right
    if not _verify_bst_internal(node.right, t_min=node.data, t_max=t_max):
        return False
    
    return True


################ Tests ####################

def test1():
    root = Node(15)
    # bst.verify_is_bst(root)
    assert verify_bst(root)
    print('####### Test 1 Passed ######')


def test2():
    root = Node(15, left=Node(16, left=Node(16)))
    # bst.verify_is_bst(root)
    assert not verify_bst(root)
    print('####### Test 2 Passed ######')


def test3():
    root = Node(15, left=Node(10, right=Node(14, right=Node(16))))
    # bst.verify_is_bst(root)
    assert not verify_bst(root)
    print('####### Test 3 Passed ######')


if __name__ == '__main__':
    test1()
    test2()
    test3()
