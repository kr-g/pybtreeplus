from pyheapfile.heap import HeapFile, to_bytes, from_bytes
from pydllfile.dllist import DoubleLinkedListFile, LINK_SIZE
from pybtreecore.btcore import BTreeElement, BTreeCoreFile
from pybtreecore.btcore import KEYS_PER_NODE, KEY_SIZE, DATA_SIZE
from pybtreecore.btnodelist import Node, NodeList

# from pybtreecore.conv import ConvertStr, ConvertInteger, ConvertFloat, ConvertComplex


class Context(object):
    def __init__(self, bpt):
        self.bpt = bpt
        self._reset()

    def _reset(self):
        self.elems = {}
        self._dirty = set()
        self._free = []

    def add(self, btelem):
        if btelem == None:
            raise Exception("None not allowed")
        pos = btelem.elem.pos
        self.elems[pos] = btelem
        return btelem

    def create_empty_list(self):
        # todo undo?
        if len(self._free) > 0:
            btelem = self._free.pop()
            print("re-use formerly freed element node")
        else:
            btelem = self.bpt.btcore.create_empty_list()
        # todo not added automatically to context !!!
        # todo not marked as dirty here yet ?
        # self.add(btelem)
        return btelem

    def free_list(self, btelem):
        self.add(btelem)
        pos = btelem.elem.pos
        if pos in self._dirty:
            self._dirty.remove(pos)
        self._free.append(btelem)

    def _read_elem(self, pos):
        if pos in self.elems:
            return self.elems[pos]
        el = self.bpt._read_elem(pos)
        return self.add(el)

    def _write_elem(self, btelem):
        pos = btelem.elem.pos
        self.elems[pos] = btelem
        self._dirty.add(pos)

    def _read_dll_elem(self, pos):
        btelem = self._read_elem(pos)
        # todo read just required parts?
        heap_node, dll_elem = btelem.node, btelem.elem
        return heap_node, dll_elem

    def _write_dll_elem(self, heap_node, dll_elem):
        pos = dll_elem.pos
        btelem = self.elems[pos]
        self._dirty.add(pos)
        btelem.node = heap_node
        btelem.elem = dll_elem

    def done(self):
        for pos, btelem in self.elems.items():
            if pos in self._dirty:
                self.bpt._write_elem(btelem)
        for btelem in self._free:
            self.bpt.btcore.heap_fd.free(btelem.node, merge_free=False)
        self._reset()

    def close(self):
        self.done()


class BPlusTree(object):
    def __init__(
        self, btcore, root_pos=0, first_pos=0, last_pos=0, conv_key=None, conv_data=None
    ):
        self.trace = False

        self.btcore = btcore
        self.link_size = btcore.fd.link_size

        self.conv_key = conv_key
        self.conv_data = conv_data

        self.root_pos = root_pos
        self.first_pos = first_pos
        self.last_pos = last_pos

    def __repr__(self):
        return (
            self.__class__.__name__
            + "( root: "
            + hex(self.root_pos)
            + " first: "
            + hex(self.first_pos)
            + " last: "
            + hex(self.last_pos)
            + " )"
        )

    # persistence methods

    def to_bytes(self):
        buf = []
        buf.extend(to_bytes(self.root_pos, self.link_size))
        buf.extend(to_bytes(self.first_pos, self.link_size))
        buf.extend(to_bytes(self.last_pos, self.link_size))
        return bytes(buf)

    def from_bytes(self, buf):
        b, buf = self._split(buf, self.link_size)
        self.root_pos = from_bytes(b)
        b, buf = self._split(buf, self.link_size)
        self.first_pos = from_bytes(b)
        b, buf = self._split(buf, self.link_size)
        self.last_pos = from_bytes(b)

        if len(buf) > 0:
            return self, buf
        return self

    # create methods

    def create_new(self):
        root = self._create_new_root()
        self.first_pos = self.root_pos
        self.last_pos = self.root_pos
        return root

    def _create_new_root(self):
        root = self.btcore.create_empty_list()
        self.root_pos = root.elem.pos
        return root

    def _create_new_root_ctx(self, ctx):
        root = ctx.create_empty_list()
        self.root_pos = root.elem.pos
        return root

    # basic io

    def _read_elem(self, pos):
        return self.btcore.read_list(
            pos, conv_key=self.conv_key, conv_data=self.conv_data
        )

    def _write_elem(self, btelem):
        return self.btcore.write_list(
            btelem, conv_key=self.conv_key, conv_data=self.conv_data
        )

    def _flush(self):
        self.btcore.heap_fd.flush()

    # iterators

    def iter_elem_first(self):
        pos = self.first_pos
        if pos == 0:
            raise Exception("not initialized")
        while pos > 0:
            btelem = self._read_elem(pos)
            yield btelem
            pos = btelem.elem.succ

    def iter_first(self):
        for btelem in self.iter_elem_first():
            for n in btelem.nodelist:
                yield n

    def iter_elem_last(self):
        pos = self.last_pos
        if pos == 0:
            raise Exception("not initialized")
        while pos > 0:
            btelem = self._read_elem(pos)
            yield btelem
            pos = btelem.elem.prev

    def iter_last(self):
        for btelem in self.iter_elem_last():
            for n in reversed(btelem.nodelist):
                yield n

    # search

    def search_node(self, key, npos=None, ctx=None):
        """search a key, or if missing return the node element to insert into"""
        if npos == None:
            if self.root_pos == 0:
                raise Exception("not initialized")
            npos = self.root_pos

        if ctx == None:
            ctx = Context(self)

        btelem = ctx._read_elem(npos)

        if len(btelem.nodelist) == 0:
            if btelem.elem.pos != self.root_pos:
                raise Exception("wrong root")
            # root node handling for less existing elements
            return None, btelem, False, ctx

        for n in btelem.nodelist:
            if n.leaf == True:
                if n.key == key:
                    return n, btelem, True, ctx
                continue
            if key <= n.key:
                return self.search_node(key, n.left)

        rpos = btelem.nodelist[-1].right
        if rpos == 0:
            return None, btelem, False, ctx

        return self.search_node(key, rpos)

    # insert methods

    def _overflow(self, btelem):
        return len(btelem.nodelist) > self.btcore.keys_per_node

    def _no_split_required(self, btelem):
        return len(btelem.nodelist) < self.btcore.keys_per_node

    def _get_split_pos(self):
        return self.btcore.keys_per_node // 2

    def _split_elem_ctx(self, btelem, ctx):
        left = ctx.create_empty_list()
        ctx.add(left)
        # re-name just for better understanding
        # but keep in mind right == btelem (until done mark)
        right = btelem
        ctx.add(right)  # useless...
        spos = self._get_split_pos()
        left.nodelist = btelem.nodelist.sliced(None, spos)
        right.nodelist = btelem.nodelist.sliced(spos, None)
        return left, right

    def insert_2_leaf(self, n, btelem, ctx=None, ctx_close=True):
        if ctx == None:
            ctx = Context(self)

        rc = self.insert_2_leaf_ctx(n, btelem, ctx)

        if ctx_close == True:
            ctx.done()

        return rc

    def insert_2_leaf_ctx(self, n, btelem, ctx):
        """inserts a leaf node, there is no check if btelem contains only leaf nodes.
        run search_insert_leaf() before to find the insert point."""

        ctx.add(btelem)

        if len(btelem.nodelist) > 0 and btelem.nodelist[0].leaf == False:
            raise Exception("insert in inner node")

        btelem.nodelist.insert(n)

        if self._no_split_required(btelem) == True:
            ctx._write_elem(btelem)
            return n, btelem, True

        left, right = self._split_elem_ctx(btelem, ctx)
        left.elem.insert_elem_before(right.elem)

        n_ins = self.insert_2_inner_ctx(left, right, ctx, key=n.key)

        ctx._write_elem(left)
        ctx._write_elem(right)

        if left.elem.prev > 0:
            prev_node, prev_elem = ctx._read_dll_elem(left.elem.prev)
            prev_elem.succ = left.elem.pos
            ctx._write_dll_elem(prev_node, prev_elem)

        if right.elem.succ > 0:
            succ_node, succ_elem = ctx._read_dll_elem(right.elem.succ)
            succ_elem.prev = right.elem.pos
            ctx._write_dll_elem(succ_node, succ_elem)

        if left.elem.prev == 0:
            self.first_pos = left.elem.pos
        if right.elem.succ == 0:
            self.last_pos = right.elem.pos

        lkey_pos = left.nodelist.find_key(n.key)
        rkey_pos = right.nodelist.find_key(n.key)
        if lkey_pos < 0 and rkey_pos < 0:
            raise Exception(
                "inserted key not found", [n, "***LEFT***", left, "***RIGHT***", right]
            )

        return n_ins, (left if lkey_pos >= 0 else right), True

    def insert_2_inner_ctx(self, left, right, ctx, key=None):
        parent_pos = left.nodelist.parent
        if parent_pos != right.nodelist.parent:
            raise Exception("parent different")

        if parent_pos == 0:

            parent = self._create_new_root_ctx(ctx)
            ctx.add(parent)

            n = Node(
                key=left.nodelist[-1].key, left=left.elem.pos, right=right.elem.pos
            )

            parent.nodelist.insert(n)

            left.nodelist.parent = parent.elem.pos
            right.nodelist.parent = parent.elem.pos

            self._update_childs_ctx(left, ctx)
            self._update_childs_ctx(right, ctx)

            ctx._write_elem(parent)
            return n

        parent = ctx._read_elem(parent_pos)

        n = Node(key=left.nodelist[-1].key, left=left.elem.pos)

        last = parent.nodelist[-1]
        if n.key > last.key:
            n.set_right(last.right)
            last.set_right(0)

        parent.nodelist.insert(n)

        if n.key > last.key:
            if parent.nodelist[-1] != n:
                raise Exception("wrong order")

        if self._no_split_required(parent) == True:
            ctx._write_elem(parent)
            return n

        pel_left, pel_right = self._split_elem_ctx(parent, ctx)

        self._update_childs_ctx(pel_left, ctx)
        self._update_childs_ctx(pel_right, ctx)

        n = self.insert_2_inner_ctx(pel_left, pel_right, ctx)

        ctx._write_elem(pel_left)
        ctx._write_elem(pel_right)
        ctx._write_elem(parent)

        return n

    # common

    def _update_childs_ctx(self, btelem, ctx):
        for n in btelem.nodelist:
            for pos in [n.left, n.right]:
                if pos > 0:
                    cn = ctx._read_elem(pos)
                    cn.nodelist.parent = btelem.elem.pos
                    ctx._write_elem(cn)

    # delete methods

    def _under_limit(self, btelem):
        return len(btelem.nodelist) <= self.btcore.keys_per_node / 3

    def _can_merge(self, left, right):
        return self._under_limit(left) and self._under_limit(right)

    def _can_borrow(self, btelem):
        return self._under_limit(btelem) == False

    def _calc_balance(self, give, recv):
        """calc how much nodes needs to be shifted to keep balance"""
        giv = len(give.nodelist)
        rcv = len(recv.nodelist)
        sam = (giv + rcv) // 2
        bal_cnt = giv - sam
        if bal_cnt >= giv:
            raise Exception("too less in giving node. swap parameter?")
        self.trace and print(
            bal_cnt, [giv, hex(give.elem.pos)], [rcv, hex(recv.elem.pos)], end=" "
        )
        return bal_cnt

    def _get_siblings_ctx(self, btelem, ctx):
        parent_pos = btelem.nodelist.parent
        if parent_pos == 0:
            raise Exception("already in root")
        parent = ctx._read_elem(parent_pos)
        separ = list(map(lambda x: x.left, parent.nodelist))
        if len(parent.nodelist) > 0:
            separ.append(parent.nodelist[-1].right)
        try:
            pos = separ.index(btelem.elem.pos)
        except:
            raise Exception("link broken", btelem, "***PARENT***", parent)
        lpos = pos - 1
        left = separ[lpos] if lpos >= 0 else 0
        rpos = pos + 1
        right = separ[rpos] if rpos < len(separ) else 0
        return left, right

    def _read_siblings_ctx(self, left_pos, right_pos, ctx):
        left = ctx._read_elem(left_pos) if left_pos > 0 else None
        right = ctx._read_elem(right_pos) if right_pos > 0 else None
        return left, right

    def delete_from_leaf(self, key, btelem, ctx=None, ctx_close=True):
        ctx = self._delete_from_ctx(key, btelem, ctx=ctx, ctx_close=ctx_close)
        return ctx

    def _delete_rebalance_ctx(self, btelem, ctx):
        left_pos, right_pos = self._get_siblings_ctx(btelem, ctx)
        left, right = self._read_siblings_ctx(left_pos, right_pos, ctx)

        left_merge = self._can_merge(left, btelem) if left != None else False
        right_merge = self._can_merge(right, btelem) if right != None else False

        left_borrow = self._can_borrow(left) if left != None else False
        right_borrow = self._can_borrow(right) if right != None else False

        if left_merge == True:
            self.trace and print(
                "ml", hex(left_pos), ">", hex(btelem.elem.pos), end=" "
            )
            self._merge_siblings_ctx(left, btelem, ctx)
        elif right_merge == True:
            self.trace and print(
                "mr", hex(right_pos), ">", hex(btelem.elem.pos), end=" "
            )
            self._merge_siblings_ctx(btelem, right, ctx)
            btelem = right
        elif left_borrow == True:
            self.trace and print("bl", hex(btelem.elem.pos), end=" ")
            self._rotate_inner_from_left_ctx(left, btelem, ctx)
        elif right_borrow == True:
            self.trace and print("br", hex(btelem.elem.pos), end=" ")
            self._rotate_inner_from_right_ctx(btelem, right, ctx)
        else:
            raise Exception("neither merge, nor borrow")
        return btelem

    def _delete_from_ctx(self, key, btelem, ctx=None, ctx_close=True):

        if ctx == None:
            ctx = Context(self)

        n = btelem.nodelist.remove_key(key)
        rpos = n.right

        if len(btelem.nodelist) == 0:
            if btelem.nodelist.parent > 0:
                raise Exception("not root")

            self.trace and print(
                "c", hex(n.left), hex(n.right), ">", hex(btelem.elem.pos), end=" "
            )

            if rpos > 0:
                # colapse by one level
                # set right as new root since left was dropped
                root = ctx._read_elem(self.root_pos)
                ctx.free_list(root)

                # todo make finally better ?
                btelem = ctx._read_elem(rpos)

                btelem.nodelist.parent = 0
                self.root_pos = rpos

                # todo make finally better ?
                rpos = 0

        else:
            if self._under_limit(btelem):
                if btelem.nodelist.parent > 0:
                    btelem = self._delete_rebalance_ctx(btelem, ctx)

        if rpos > 0:
            if len(btelem.nodelist) == 0:
                raise Exception("last elem", [key, hex(rpos), btelem])
            if btelem.nodelist[-1].right != 0:
                raise Exception("right found", hex(btelem.elem.pos))
            btelem.nodelist[-1].set_right(rpos)
            # self._update_childs_ctx(btelem, ctx)

        ctx._write_elem(btelem)

        if ctx_close == True:
            ctx.done()

        return ctx

    def _rotate_inner_from_right_ctx(self, left, right, ctx):
        """rotate nodes from right to left"""
        to_move = self._calc_balance(right, left)
        for i in range(0, to_move):
            n = right.nodelist.pop(0)
            left.nodelist.insert(n)
            if left.nodelist[-1] != n:
                raise Exception("wrong order")

        parent = ctx._read_elem(left.nodelist.parent)

        pn = list(filter(lambda x: x.left == left.elem.pos, parent.nodelist))[0]
        pn.key = n.key

        self._update_childs_ctx(left, ctx)
        # self._update_childs_ctx(right, None, ctx)
        self._update_childs_ctx(parent, ctx)

        ctx._write_elem(left)
        ctx._write_elem(right)
        ctx._write_elem(parent)

    def _rotate_inner_from_left_ctx(self, left, right, ctx):
        """rotate nodes from left to right"""
        to_move = self._calc_balance(left, right)
        for i in range(0, to_move):
            n = left.nodelist.pop(-1)
            right.nodelist.insert(n)
            if right.nodelist[0] != n:
                raise Exception("wrong order")

        parent = ctx._read_elem(right.nodelist.parent)

        pn = list(filter(lambda x: x.left == left.elem.pos, parent.nodelist))[0]
        pn.key = left.nodelist[-1].key

        pn = list(
            filter(
                lambda x: x.left == right.elem.pos or x.right == right.elem.pos,
                parent.nodelist,
            )
        )
        pn = pn[0]
        if pn.right == 0:
            pn.key = right.nodelist[-1].key

        # self._update_childs_ctx(left, None, ctx)
        self._update_childs_ctx(right, ctx)
        self._update_childs_ctx(parent, ctx)

        ctx._write_elem(left)
        ctx._write_elem(right)
        ctx._write_elem(parent)

    def _merge_siblings_ctx(self, left, right, ctx):
        """merge left to right, drop left in parent"""
        prev_node = None
        if left.elem.prev > 0:
            prev_node, prev_elem = ctx._read_dll_elem(left.elem.prev)
            prev_elem.succ = left.elem.succ
        right.elem.prev = left.elem.prev

        for n in left.nodelist:
            right.nodelist.insert(n)
        left.nodelist.clear()

        if self._no_split_required(right) == False:
            raise Exception("nodelist overflow")

        parent = ctx._read_elem(left.nodelist.parent)
        pn = list(filter(lambda x: x.left == left.elem.pos, parent.nodelist))[0]

        if prev_node != None:
            ctx._write_dll_elem(prev_node, prev_elem)
        # ctx._write_elem(left)
        ctx.free_list(left)

        if self.first_pos == left.elem.pos:
            self.first_pos = left.elem.succ

        ctx._write_elem(right)
        self._update_childs_ctx(right, ctx)

        self.trace and print("d", pn.key, hex(parent.elem.pos), end=" ")
        self._delete_from_ctx(pn.key, parent, ctx=ctx, ctx_close=False)
