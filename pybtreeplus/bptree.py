from pyheapfile.heap import HeapFile, to_bytes, from_bytes
from pydllfile.dllist import DoubleLinkedListFile, LINK_SIZE
from pybtreecore.btcore import BTreeElement, BTreeCoreFile
from pybtreecore.btcore import KEYS_PER_NODE, KEY_SIZE, DATA_SIZE
from pybtreecore.btnodelist import Node, NodeList

# from pybtreecore.conv import ConvertStr, ConvertInteger, ConvertFloat, ConvertComplex


class Context(object):
    def __init__(self, bpt):
        self.bpt = bpt
        self._dirty = set()
        self._reset()

    def _reset(self):
        self.elems = {}

    def add(self, btelem):
        pos = btelem.elem.pos
        self.elems[pos] = btelem
        return btelem

    def create_empty_list(self):
        # todo undo?
        btelem = self.bpt.btcore.create_empty_list()
        # todo not added automatically to context !!!
        # self.add(btelem)
        return btelem

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
        self._reset()

    def close(self):
        self.done()

    def __del__(self):
        self.close()


class BPlusTree(object):
    def __init__(
        self, btcore, root_pos=0, first_pos=0, last_pos=0, conv_key=None, conv_data=None
    ):
        self.btcore = btcore
        self.link_size = btcore.fd.link_size

        self.conv_key = conv_key
        self.conv_data = conv_data

        self.root_pos = root_pos
        self.first_pos = first_pos
        self.last_pos = last_pos

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

    def search_node(self, key, npos=None, ctx=None):
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

    def search_insert_leaf(self, key, npos=None, ctx=None):
        if npos == None:
            if self.root_pos == 0:
                raise Exception("not initialized")
            npos = self.root_pos

        if ctx == None:
            ctx = Context(self)

        btelem = ctx._read_elem(npos)

        if len(btelem.nodelist) == 0:
            # empty root
            return btelem, ctx

        for n in btelem.nodelist:
            if n.leaf == True:
                return btelem, ctx
            if key <= n.key:
                return self.search_insert_leaf(key, n.left)

        rpos = btelem.nodelist[-1].right
        return self.search_insert_leaf(key, rpos)

    def _no_split_required(self, btelem):
        return len(btelem.nodelist) < self.btcore.keys_per_node

    def _get_split_pos(self):
        return self.btcore.keys_per_node // 2

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

        # keep together
        left = ctx.create_empty_list()
        ctx.add(left)
        # re-name just for better understanding
        # but keep in mind right == btelem (until done mark)
        right = btelem
        ctx.add(right)  # useless...
        spos = self._get_split_pos()
        left.nodelist = btelem.nodelist.sliced(None, spos)
        right.nodelist = btelem.nodelist.sliced(spos, None)
        # done

        left.elem.insert_elem_before(right.elem)

        p_left, p_right, n_ins = self.insert_2_inner_ctx(left, right, ctx, key=n.key)

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

            self._update_childs_ctx(left, n.key, ctx)
            self._update_childs_ctx(right, n.key, ctx)

            ctx._write_elem(parent)
            return parent.elem.pos, parent.elem.pos, n

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
            return parent.elem.pos, parent.elem.pos, n

        # keep together
        pel_left = ctx.create_empty_list()
        ctx.add(pel_left)
        # re-name just for better understanding
        # but keep in mind right == btelem (until done mark)
        pel_right = parent
        ctx.add(pel_right)  # useless...
        spos = self._get_split_pos()
        pel_left.nodelist = parent.nodelist.sliced(None, spos)
        pel_right.nodelist = parent.nodelist.sliced(spos, None)
        # done

        self._update_childs_ctx(pel_left, n.key, ctx)
        self._update_childs_ctx(pel_right, n.key, ctx)

        p_left, p_right, n = self.insert_2_inner_ctx(pel_left, pel_right, ctx)

        ctx._write_elem(pel_left)
        ctx._write_elem(pel_right)
        ctx._write_elem(parent)

        return pel_left.elem.pos, pel_right.elem.pos, n

    def _update_childs_ctx(self, nl, key, ctx):
        for n in nl.nodelist:
            if n.left == 0:
                continue
            cn = ctx._read_elem(n.left)
            cn.nodelist.parent = nl.elem.pos
            ctx._write_elem(cn)
        rpos = nl.nodelist[-1].right
        if rpos > 0:
            cn = ctx._read_elem(rpos)
            cn.nodelist.parent = nl.elem.pos
            ctx._write_elem(cn)
