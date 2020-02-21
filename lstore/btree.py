class BTreeNode(object):
    def __init__(self, t):
        self.leaf = True
        self.keys = []
        self.values = []
        self.t = t

    def add(self, key, value):
        '''
        Adds a key-value pair to the node.
        '''
        if not self.keys:
            self.keys.append(key)
            self.values.append([value])

        for i, item in enumerate(self.keys):
            if key == item:
                self.values[i].append(value)
                break

            elif key < item:
                self.keys = self.keys[:i] + [key] + self.keys[i:]
                self.values = self.values[:i] + [[value]] + self.values[i:]
                break

            elif i + 1 == len(self.keys):
                self.keys.append(key)
                self.values.append([value])
                break

    def split(self):
        '''
        Splits the node into two and stores them as child nodes.
        '''

        left = BTreeNode(self.t)
        right = BTreeNode(self.t)
        mid = self.order / 2

        left.keys = self.keys[:mid]
        left.values = self.values[:mid]

        right.keys = self.keys[mid:]
        right.values = self.values[mid:]

        self.keys = [right.keys[0]]
        self.values = [left, right]
        self.leaf = False

    def isfull(self):
        return len(self.keys) == self.t

    def show(self, counter=0):
        '''
        Prints the keys at each level.
        '''
        print(counter, str(self.keys))

        if not self.leaf:
            for item in self.values:
                item.show(counter + 1)


class BPTree(object):
    def __init__(self, t=8):
        self.root = BTreeNode(t)

    def _find(self, node, key):
        '''
        For a given node and key, returns the index where the key should be
        inserted and the list of values at that index.
        '''
        for i, item in enumerate(node.keys):
            if key < item:
                return node.values[i], i

        return node.values[i + 1], i + 1

    def _merge(self, parent, child, index):
        '''
        For a parent and child node, extract a pivot from the child to be
        inserted into the keys of the parent. Insert the values from the child
        into the values of the parent.
        '''
        parent.values.pop(index)
        pivot = child.keys[0]

        for i, item in enumerate(parent.keys):
            if pivot < item:
                parent.keys = parent.keys[:i] + [pivot] + parent.keys[i:]
                parent.values = parent.values[:i] + child.values + parent.values[i:]
                break

            elif i + 1 == len(parent.keys):
                parent.keys += [pivot]
                parent.values += child.values
                break

    def insert(self, key, value):
        '''
        Inserts a key-value pair after traversing to a leaf node. If the leaf
        node is full, split the leaf node into two.
        '''
        parent = None
        child = self.root

        while not child.leaf:
            parent = child
            child, index = self._find(child, key)

        child.add(key, value)

        if child.is_full():
            child.split()

            if parent and not parent.is_full():
                self._merge(parent, child, index)

    def retrieve(self, key):
        '''
        Returns a value for a given key, and None if the key does not exist.
        '''
        child = self.root

        while not child.leaf:
            child, index = self._find(child, key)

        for i, item in enumerate(child.keys):
            if key == item:
                return child.values[i]

        return None

    def show(self):
        '''
        Prints the keys at each level.
        '''
        self.root.show()
