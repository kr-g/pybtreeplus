import unittest
import random

from pybtreeplus.bptree import HeapFile, BPlusTree, BTreeCoreFile, Node, NodeList
from pybtreecore.conv import ConvertStr, ConvertInteger, ConvertFloat, ConvertComplex

fnam = "mytest.hpf"


class BTreePlusDefaultTestCase(unittest.TestCase):
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

        _, _, rc = bpt.search_node(ntxt)
        self.assertFalse(rc, [ntxt, rc])

        i_btelem = bpt.search_insert_leaf(ntxt)
        self.assertTrue(i_btelem != None, [ntxt, i_btelem])

        n = Node(key=ntxt, data=ndat)
        n, cngbtelem, rc = bpt.insert_2_leaf(n, i_btelem)
        self.assertTrue(rc, [ntxt, n, cngbtelem])

        self._test_tree_inner(ref=ntxt)

        node, ex_btelem, rc = bpt.search_node(ntxt)
        self.assertTrue(rc, ntxt)
        self.assertEqual(node.key, ntxt)
        if chk:
            self._check_leaf(i_btelem)
            self._check_leaf(cngbtelem)

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

    # test

    def test_0000_insert(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = self._test_data_insert_and_chk(0)
        samples.extend(self._test_data_insert_and_chk(1))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)
        self._test_iter(samples)

    def test_0010_insert_no_split(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        for i in range(0, btcore.keys_per_node - 1):
            samples.extend(self._test_data_insert_and_chk(i))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)
        self._test_iter(samples)

    def test_0020_insert_split(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        for i in range(0, btcore.keys_per_node - 1):
            samples.extend(self._test_data_insert_and_chk(i))
        samples.extend(self._test_data_insert_and_chk(btcore.keys_per_node))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)
        self._test_iter(samples)

    def test_0030_insert_left_split(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        for i in range(0, btcore.keys_per_node - 1):
            samples.extend(self._test_data_insert_and_chk(i, mult=100))
        for i in range(0, btcore.keys_per_node):
            samples.extend(self._test_data_insert_and_chk(i, mult=1, offs=1))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)
        self._test_iter(samples)

    def test_0040_insert_right_split(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        for i in range(0, btcore.keys_per_node - 1):
            samples.extend(self._test_data_insert_and_chk(i, mult=100))
        for i in range(0, btcore.keys_per_node):
            samples.extend(self._test_data_insert_and_chk(i, mult=1, offs=101))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)
        self._test_iter(samples)

    def test_0050_insert_middle_split(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        for i in range(0, btcore.keys_per_node - 1):
            samples.extend(self._test_data_insert_and_chk(i, mult=100))
        for i in range(0, btcore.keys_per_node):
            samples.extend(self._test_data_insert_and_chk(i, mult=1, offs=51))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)
        self._test_iter(samples)

    def test_0060_insert_left_split_2(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        for i in range(0, btcore.keys_per_node - 1):
            samples.extend(self._test_data_insert_and_chk(i, mult=100))
        for i in range(0, btcore.keys_per_node):
            samples.extend(self._test_data_insert_and_chk(i, mult=1, offs=1))
        for i in range(0, btcore.keys_per_node):
            samples.extend(self._test_data_insert_and_chk(i, mult=1, offs=21))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)
        self._test_iter(samples)

    def test_0070_insert_split_leaf_2_insert_last(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        for i in range(0, btcore.keys_per_node - 1):
            samples.extend(self._test_data_insert_and_chk(i, mult=100))
        for i in range(0, btcore.keys_per_node):
            samples.extend(self._test_data_insert_and_chk(i, mult=1, offs=201))
        for i in range(0, btcore.keys_per_node):
            samples.extend(self._test_data_insert_and_chk(i, mult=1, offs=301))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)
        self._test_iter(samples)

    def test_0080_insert_split_leaf_2_insert_middle(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        for i in range(0, btcore.keys_per_node - 1):
            samples.extend(self._test_data_insert_and_chk(i, mult=100))
        for i in range(0, btcore.keys_per_node):
            samples.extend(self._test_data_insert_and_chk(i, mult=1, offs=31))
        for i in range(0, btcore.keys_per_node):
            samples.extend(self._test_data_insert_and_chk(i, mult=1, offs=71))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)
        self._test_iter(samples)

    def test_0090_insert_split_sequence_large(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        maxn = btcore.keys_per_node * 8 * 8
        for i in range(0, maxn):
            samples.extend(self._test_data_insert_and_chk(i))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)
        self._test_iter(samples)

    def test_0100_insert_split_sequence_predef_rand(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        elems = self.__get_elems()

        for i in elems:
            samples.extend(self._test_data_insert_and_chk(i, mult=1))

        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)
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

    def test_0110_insert_split_sequence_large_predef_rand(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        if False:
            maxn = btcore.keys_per_node * 8 * 8
            elems = list(range(0, maxn))
            random.shuffle(elems)
        else:
            elems = self.__get_elems_big()

        for i in elems:
            samples.extend(self._test_data_insert_and_chk(i, mult=1))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)
        self._test_iter(samples)

    def test_0120_insert_split_sequence_large_full_rand(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        maxn = btcore.keys_per_node * 8 * 8
        elems = list(range(0, maxn))
        random.shuffle(elems)

        for i in elems:
            samples.extend(self._test_data_insert_and_chk(i, mult=1))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)
        self._test_iter(samples)

    def __test_0130_insert_split_sequence_large_full_rand_even_bigger(self):
        hpf, btcore, bpt, node0, root = self.para

        samples = []
        maxn = btcore.keys_per_node * 8 * 8 * 8
        elems = list(range(0, maxn))
        random.shuffle(elems)

        for i in elems:
            samples.extend(self._test_data_insert_and_chk(i, mult=1))
        self.assertTrue(samples != None)
        self.assertTrue(len(samples) > 0)
        self._test_iter(samples)

    def _test_z_nodelist(self):
        parent = NodeList()
        for i in range(0, 10):
            parent.insert(Node(key=str(i).zfill(3), data=i))
        selem = parent.split_at(5)
        print(parent)
        print(selem)

    def __get_elems_big(self):
        import json

        s = """[398, 935, 417, 677, 314, 615, 101, 785, 590, 579, 235, 239, 43, 783, 653,
                848, 291, 651, 160, 230, 601, 891, 103, 312, 897, 434, 27, 229, 270, 220, 873, 370,
                437, 136, 833, 298, 877, 984, 861, 711, 888, 141, 536, 29, 594, 265, 420, 803, 638,
                175, 713, 372, 857, 556, 513, 529, 519, 209, 143, 180, 4, 1008, 774, 890, 146, 545,
                683, 469, 293, 348, 432, 914, 1, 28, 202, 926, 36, 896, 696, 316, 259, 507, 818,
                602, 548, 364, 963, 974, 652, 839, 337, 740, 45, 54, 705, 109, 264, 971, 830, 490,
                739, 718, 749, 930, 201, 729, 665, 945, 286, 646, 164, 647, 645, 853, 38, 87, 517,
                476, 391, 167, 283, 822, 924, 464, 732, 472, 284, 612, 390, 812, 673, 691, 292,
                444, 119, 625, 775, 558, 409, 817, 499, 624, 622, 349, 727, 671, 871, 832, 448,
                492, 428, 237, 415, 212, 389, 1004, 461, 367, 431, 961, 614, 241, 878, 196, 419,
                742, 575, 898, 106, 670, 1011, 130, 588, 297, 909, 473, 68, 114, 384, 616, 610,
                408, 33, 496, 708, 491, 94, 849, 550, 916, 660, 310, 688, 919, 438, 257, 983, 1015,
                725, 1017, 149, 361, 1019, 982, 242, 162, 994, 439, 422, 600, 83, 285, 441, 102,
                430, 568, 245, 796, 198, 700, 597, 769, 176, 253, 508, 500, 133, 1001, 138, 296,
                240, 219, 787, 17, 995, 347, 836, 866, 447, 305, 85, 946, 244, 823, 948, 678, 632,
                704, 227, 876, 42, 79, 192, 977, 726, 789, 120, 844, 596, 92, 827, 182, 463, 856,
                340, 84, 754, 837, 57, 922, 342, 676, 710, 667, 78, 3, 250, 996, 561, 756, 582,
                452, 786, 112, 53, 992, 692, 197, 863, 282, 964, 199, 166, 939, 991, 331, 466, 985,
                34, 609, 815, 860, 1002, 528, 40, 482, 144, 920, 363, 74, 326, 524, 702, 14, 854,
                685, 96, 88, 71, 543, 266, 679, 424, 301, 47, 846, 650, 111, 793, 290, 397, 256,
                147, 745, 753, 577, 289, 834, 564, 320, 254, 764, 93, 593, 706, 465, 644, 12, 859,
                495, 958, 332, 399, 553, 412, 5, 962, 510, 838, 107, 52, 627, 373, 979, 932, 874,
                744, 928, 698, 1020, 989, 902, 937, 46, 663, 938, 450, 618, 75, 117, 1023, 746, 82,
                721, 566, 365, 44, 407, 385, 554, 757, 61, 423, 760, 788, 317, 748, 280, 943, 386,
                826, 494, 376, 913, 693, 998, 515, 261, 6, 549, 56, 203, 246, 921, 886, 231, 552,
                319, 76, 714, 731, 443, 973, 730, 648, 617, 122, 870, 810, 351, 626, 791, 410, 217,
                684, 238, 637, 145, 300, 456, 782, 630, 1005, 435, 140, 557, 313, 154, 104, 865,
                723, 790, 362, 735, 664, 806, 378, 185, 595, 728, 195, 232, 208, 752, 455, 717,
                224, 278, 1022, 360, 1013, 251, 222, 1018, 416, 306, 375, 559, 520, 879, 968, 249,
                875, 1014, 681, 516, 1021, 1006, 976, 965, 967, 350, 356, 13, 477, 907, 395, 184,
                563, 206, 484, 157, 281, 183, 900, 322, 214, 163, 583, 804, 699, 682, 901, 772, 2,
                451, 225, 522, 210, 923, 24, 127, 567, 1016, 894, 16, 118, 824, 933, 514, 273, 736,
                831, 855, 30, 659, 918, 512, 462, 623, 260, 947, 382, 191, 467, 243, 228, 629, 825,
                949, 346, 252, 371, 805, 72, 814, 485, 86, 454, 842, 526, 460, 546, 966, 194, 453,
                21, 584, 35, 174, 709, 324, 487, 236, 1003, 634, 294, 885, 868, 161, 858, 621, 62,
                539, 123, 502, 323, 137, 343, 328, 190, 654, 308, 421, 457, 569, 695, 722, 934,
                917, 542, 381, 640, 784, 908, 486, 636, 404, 394, 159, 862, 955, 50, 178, 67, 80,
                315, 272, 344, 633, 766, 7, 388, 216, 359, 158, 872, 121, 741, 737, 811, 887, 931,
                279, 493, 126, 498, 733, 95, 23, 807, 942, 869, 751, 171, 26, 765, 368, 355, 383,
                795, 377, 592, 797, 867, 689, 675, 135, 186, 763, 707, 635, 674, 339, 940, 587,
                506, 481, 9, 521, 895, 70, 480, 941, 715, 694, 173, 997, 269, 288, 585, 801, 586,
                669, 357, 1012, 177, 325, 233, 816, 302, 116, 1007, 589, 759, 58, 19, 716, 980,
                864, 686, 802, 304, 747, 929, 97, 333, 672, 418, 124, 738, 366, 341, 387, 820, 330,
                258, 1009, 64, 327, 60, 153, 777, 794, 200, 813, 719, 773, 41, 73, 468, 287, 403,
                889, 555, 649, 142, 904, 358, 150, 970, 425, 65, 734, 478, 641, 49, 987, 295, 318,
                501, 353, 518, 414, 821, 211, 915, 604, 446, 950, 535, 936, 379, 912, 345, 105,
                148, 505, 335, 767, 798, 578, 445, 374, 413, 369, 957, 845, 218, 32, 334, 479, 132,
                255, 993, 188, 393, 31, 99, 843, 497, 139, 762, 852, 234, 781, 534, 530, 562, 400,
                892, 18, 607, 809, 179, 969, 680, 538, 271, 131, 25, 115, 170, 307, 442, 396, 277,
                828, 523, 541, 771, 459, 426, 565, 531, 527, 215, 770, 599, 910, 656, 10, 475, 55,
                470, 51, 436, 321, 274, 690, 952, 406, 8, 20, 643, 402, 573, 840, 151, 69, 959,
                808, 779, 978, 658, 66, 819, 248, 893, 263, 591, 401, 128, 15, 571, 799, 835, 881,
                223, 100, 576, 572, 598, 352, 954, 743, 986, 613, 354, 207, 537, 48, 165, 792, 666,
                724, 91, 169, 990, 471, 560, 129, 309, 778, 776, 108, 511, 213, 880, 661, 267, 22,
                975, 829, 488, 187, 449, 59, 299, 503, 205, 755, 509, 113, 780, 37, 606, 81, 338,
                687, 540, 303, 906, 221, 489, 474, 532, 605, 972, 311, 611, 77, 193, 847, 761, 758,
                655, 701, 168, 574, 134, 657, 440, 925, 226, 944, 697, 411, 39, 668, 1000, 63, 903,
                720, 620, 712, 0, 800, 960, 639, 988, 181, 525, 429, 125, 275, 981, 483, 504, 380,
                999, 458, 911, 392, 551, 951, 750, 276, 608, 581, 768, 152, 631, 405, 155, 189, 89,
                883, 841, 619, 851, 628, 262, 110, 156, 580, 98, 899, 11, 570, 247, 336, 953, 642,
                427, 172, 90, 547, 882, 433, 905, 703, 603, 544, 884, 533, 1010, 956, 329, 268,
                927, 662, 204, 850]"""
        return json.loads(s)
