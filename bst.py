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


def verify_is_bst(root: Node):
    last_data = None
    for data in traverse_in_order(root):
        if not last_data:
            last_data = data
            continue

        if last_data > data:
            return False

        last_data = data

    return True


def traverse_in_order(root: Node):
    if not root:
        return

    if root.left:
        yield from traverse_in_order(root.left)

    yield root.data

    if root.right:
        yield from traverse_in_order(root.right)
