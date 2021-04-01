import unittest
import random

from pybtreeplus.bptree import HeapFile, BPlusTree, BTreeCoreFile, Node, NodeList
from pybtreecore.conv import ConvertStr, ConvertInteger, ConvertFloat, ConvertComplex

fnam = "mytest.hpf"


class BTreePlusDeleteTestCase(unittest.TestCase):
    def setUp(self):
        self.para = self._create_heap()

    def tearDown(self):
        hpf, btcore, bpt, node0, root = self.para
        hpf.write_node(node0, bpt.to_bytes())
        print("b+tree", bpt)
        print("-" * 37)
        hpf.close()

    # helper

    def _create_heap(self):
        hpf = HeapFile(fnam).create()
        hpf.close()

        hpf = HeapFile(fnam).open()

        node0 = hpf.alloc(0x50, data="not empty first node".encode())
        self.assertNotEqual(node0, None)

        btcore = BTreeCoreFile(hpf)  # , keys_per_node=3)

        conv_key = ConvertStr()
        conv_data = ConvertFloat()

        bpt = BPlusTree(btcore=btcore, conv_key=conv_key, conv_data=conv_data)

        root = bpt.create_new()

        return hpf, btcore, bpt, node0, root

    def _iter_all(self, print_=True, func=None):
        raise Exception("not used")
        hpf, btcore, bpt, node0, root = self.para
        for n in bpt.iter_first():
            if print_:
                print("iter", n)
            if func:
                try:
                    func(n)
                    continue
                except:
                    pass
                func()

    def _check_leaf(self, btelem):
        for n in btelem.nodelist:
            if n.leaf == False or n.left != 0 or n.right != 0 or (n.data == None):
                raise Exception(btelem)

    def _check_inner(self, btelem):
        hpf, btcore, bpt, node0, root = self.para

        for n in btelem.nodelist:
            if n.leaf == True or n.left == 0 or n.data != None:
                raise Exception(btelem)

    def _test_data(self, i, mult=10, offs=0):
        return "hello" + str(i * mult + offs).zfill(5), i

    def _test_data_insert_and_chk(self, i, mult=10, offs=0, chk=True, pr=False):
        hpf, btcore, bpt, node0, root = self.para

        samples = []

        ntxt, ndat = self._test_data(i, mult=mult, offs=offs)
        samples.append((ntxt, ndat))

        # here a context is returned which can be reused in
        # following calls to speed up performance
        _, i_btelem, rc, ctx = bpt.search_node(ntxt)
        self.assertFalse(rc, [ntxt, rc])
        self.assertTrue(i_btelem != None, [ntxt, i_btelem])

        n = Node(key=ntxt, data=ndat)
        n, cngbtelem, rc = bpt.insert_2_leaf(n, i_btelem, ctx=ctx)
        self.assertTrue(rc, [ntxt, n, cngbtelem])

        # checking the _whole_ tree afer only one insert
        # this slows down the overall test run time
        self._test_tree_inner(ref=ntxt)

        node, ex_btelem, rc, ctx = bpt.search_node(ntxt)
        del ctx

        self.assertTrue(rc, ntxt)
        self.assertEqual(node.key, ntxt)

        return samples

    def _test_iter(self, samples=None):
        hpf, btcore, bpt, node0, root = self.para

        if samples != None:
            samples.sort(key=lambda x: x[0])

        last = None
        icnt = 0
        scnt = 0
        for n in bpt.iter_first():
            if last != None:
                self.assertTrue(n.key > last.key, [n, last])
            last = n

            if samples != None:
                key, data = samples.pop(0)
                self.assertTrue(
                    key == n.key,
                    ["key: expect", n.key, "found", key, "samples", samples],
                )
                self.assertTrue(data == n.data, ["data: expect", n.data, "found", data])

                scnt += 1
            icnt += 1
        self.assertEqual(scnt, icnt, "sample != iter count")

    def _test_tree_inner(self, npos=None, parent_pos=None, ref=None):
        hpf, btcore, bpt, node0, root = self.para

        if npos == None:
            npos = bpt.root_pos

        btelem = bpt._read_elem(npos)

        if len(btelem.nodelist) == 0:
            return

        if parent_pos != None and btelem.nodelist.parent != parent_pos:
            raise Exception("parent broken", btelem, "expected", hex(parent_pos), ref)

        if btelem.nodelist[0].leaf:
            self._check_leaf(btelem)
            return

        self._check_inner(btelem)

        for n in btelem.nodelist:
            self._test_tree_inner(n.left, parent_pos=btelem.elem.pos, ref=ref)

        rpos = btelem.nodelist[-1].right
        if btelem.elem.pos == bpt.root_pos:
            self._test_tree_inner(rpos, parent_pos=btelem.elem.pos, ref=ref)

    # deletion test cases

    def test_0200_del_no_split(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        for i in range(0, btcore.keys_per_node - 1):
            samples.extend(self._test_data_insert_and_chk(i))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)

        for it in [0, 7, 14]:
            ntxt, i = self._test_data(it, mult=10)
            samples.remove((ntxt, i))

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertTrue(rc, [ntxt, rc])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            bpt.delete_from_leaf(ntxt, i_btelem)

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertFalse(rc, [ntxt, rc])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            self._test_tree_inner(ref=ntxt)

        self.assertTrue(len(i_btelem.nodelist), 12)

        self._test_iter(samples)

    def test_0210_del_split_no_parent_remove(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        for i in range(0, btcore.keys_per_node * 2):
            samples.extend(self._test_data_insert_and_chk(i))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)
        # self._test_iter(samples)

        for it in [0, 7, 14, 16, 23, 31]:
            ntxt, i = self._test_data(it, mult=10)
            samples.remove((ntxt, i))

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertTrue(rc, [ntxt, rc])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            bpt.delete_from_leaf(ntxt, i_btelem)

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertFalse(rc, [ntxt, rc])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            self._test_tree_inner(ref=ntxt)

        self._test_iter(samples)

    def test_0220_del_split_middle_parent_remove(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        for i in range(0, btcore.keys_per_node * 2):
            samples.extend(self._test_data_insert_and_chk(i))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)

        for it in [7, 8, 9, 10, 11, 12, 13, 14, 15]:
            ntxt, i = self._test_data(it, mult=10)
            samples.remove((ntxt, i))

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertTrue(rc, [ntxt, rc])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            bpt.delete_from_leaf(ntxt, i_btelem)

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertFalse(rc, [ntxt, rc])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            self._test_tree_inner(ref=ntxt)

        self._test_iter(samples)

    def test_0230_del_split_middle_left_remove(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        for i in range(0, btcore.keys_per_node * 2):
            samples.extend(self._test_data_insert_and_chk(i))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)

        for it in [0, 1, 2, 3, 4, 5, 6, 7]:
            ntxt, i = self._test_data(it, mult=10)
            samples.remove((ntxt, i))

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertTrue(rc, [ntxt, rc])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            bpt.delete_from_leaf(ntxt, i_btelem)

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertFalse(rc, [ntxt, rc])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            self._test_tree_inner(ref=ntxt)

        self._test_iter(samples)

    def test_0240_del_split_last_left_remove(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        for i in range(0, btcore.keys_per_node * 2):
            samples.extend(self._test_data_insert_and_chk(i))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)

        print("b+tree before", bpt)

        for it in [16, 17, 18, 19, 20, 21, 22, 23]:
            ntxt, i = self._test_data(it, mult=10)
            samples.remove((ntxt, i))

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertTrue(rc, [ntxt, rc])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            bpt.delete_from_leaf(ntxt, i_btelem)

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertFalse(rc, [ntxt, rc])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            self._test_tree_inner(ref=ntxt)

        self._test_iter(samples)

    def test_0250_del_split_last_right_remove(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        for i in range(0, btcore.keys_per_node * 2):
            samples.extend(self._test_data_insert_and_chk(i))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)

        print("b+tree before", bpt)

        for it in [24, 25, 26, 27, 28, 29, 30, 31]:
            ntxt, i = self._test_data(it, mult=10)
            samples.remove((ntxt, i))

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertTrue(rc, [ntxt, rc])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            bpt.delete_from_leaf(ntxt, i_btelem)

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertFalse(rc, [ntxt, rc])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            self._test_tree_inner(ref=ntxt)

        self._test_iter(samples)

    def test_0260_del_no_split_remove_all(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        for i in range(0, btcore.keys_per_node - 1):
            samples.extend(self._test_data_insert_and_chk(i))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)

        for i in range(0, btcore.keys_per_node - 1):
            ntxt, i = self._test_data(i, mult=10)
            samples.remove((ntxt, i))

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertTrue(rc, [ntxt, rc])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            bpt.delete_from_leaf(ntxt, i_btelem)

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertFalse(rc, [ntxt, rc])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            self._test_tree_inner(ref=ntxt)

        self._test_iter(samples)

    def test_0270_del_split_remove_all(self):
        hpf, btcore, bpt, node0, root = self.para

        max_loop = 8

        samples = []
        for i in range(0, btcore.keys_per_node * max_loop):
            samples.extend(self._test_data_insert_and_chk(i))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)

        print("b+tree before", bpt)

        for i in range(0, btcore.keys_per_node * max_loop):
            ntxt, i = self._test_data(i, mult=10)
            samples.remove((ntxt, i))

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertTrue(rc, [ntxt, rc])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            bpt.delete_from_leaf(ntxt, i_btelem)

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertFalse(rc, [ntxt, rc, samples])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            self._test_tree_inner(ref=ntxt)

        self._test_iter(samples)

    def test_0280_del_split_sequence_predef_rand_seq(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        elems = self.__get_elems()

        for i in elems:
            samples.extend(self._test_data_insert_and_chk(i, mult=1))

        dels = 0
        for i in range(0, len(elems)):
            ntxt, i = self._test_data(i, mult=1)
            samples.remove((ntxt, i))

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertTrue(rc, [ntxt, rc])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            bpt.delete_from_leaf(ntxt, i_btelem)

            _, i_btelem, rc, ctx = bpt.search_node(ntxt)
            self.assertFalse(rc, [ntxt, rc, samples])
            self.assertTrue(i_btelem != None, [ntxt, i_btelem])

            self._test_tree_inner(ref=ntxt + " " + str(dels))
            dels += 1

        self._test_iter(samples)

    def __get_elems(self):
        import json

        s = """[91, 15, 93, 90, 176, 194, 136, 85, 112, 55, 74, 178, 119, 9, 67, 191, 8, 11,
                177, 153, 69, 70, 114, 185, 151, 181, 109, 183, 189, 60, 101, 87, 29, 172,
                159, 56, 145, 28, 184, 5, 64, 100, 97, 12, 75, 68, 108, 45, 130, 111, 43, 19,
                50, 104, 149, 156, 135, 120, 167, 98, 53, 192, 14, 79, 147, 17, 51, 179, 128,
                20, 35, 110, 34, 32, 122, 2, 155, 31, 41, 170, 165, 134, 37, 190, 150, 25, 39,
                66, 62, 71, 143, 198, 105, 199, 180, 47, 92, 18, 33, 48, 140, 23, 30, 196, 182,
                27, 139, 83, 174, 57, 22, 59, 144, 124, 123, 72, 58, 102, 142, 10, 46, 157, 42,
                3, 115, 4, 77, 106, 26, 113, 117, 132, 24, 137, 103, 193, 7, 61, 169, 118, 0,
                195, 16, 63, 154, 73, 13, 40, 88, 76, 160, 86, 158, 54, 125, 94, 38, 162, 164,
                163, 65, 131, 197, 36, 127, 84, 81, 126, 129, 80, 187, 168, 121, 161, 173, 171,
                116, 141, 52, 107, 188, 133, 148, 166, 89, 146, 175, 21, 6, 95, 82, 1, 99, 152,
                138, 49, 96, 44, 186, 78]"""
        return json.loads(s)
