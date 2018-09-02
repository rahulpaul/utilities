import bst


def test1():
    root = bst.Node(15)
    # bst.verify_is_bst(root)
    assert bst.verify_is_bst(root)


def test2():
    root = bst.Node(15, left=bst.Node(16, left=bst.Node(16)))
    # bst.verify_is_bst(root)
    assert not bst.verify_is_bst(root)


def test3():
    root = bst.Node(15, left=bst.Node(10, right=bst.Node(14, right=bst.Node(16))))
    # bst.verify_is_bst(root)
    assert not bst.verify_is_bst(root)
