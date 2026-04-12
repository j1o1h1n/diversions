package com.helianthi.jscream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PackedIntListsTest {
    @Test
    void packsMultipleIntLists() {
        PackedIntLists lists = new PackedIntLists(8);

        int first = lists.list();
        lists.append(11);
        lists.append(22);

        int second = lists.list();
        lists.append(33);

        assertEquals(0, first);
        assertEquals(3, second);
        assertEquals(2, lists.size(first));
        assertEquals(11, lists.get(first, 0));
        assertEquals(22, lists.get(first, 1));
        assertEquals(1, lists.size(second));
        assertEquals(33, lists.get(second, 0));

        lists.reset();

        int third = lists.list();
        lists.append(44);

        assertEquals(0, third);
        assertEquals(1, lists.size(third));
        assertEquals(44, lists.get(third, 0));
    }

    @Test
    void growsBeyondInitialCapacity() {
        PackedIntLists lists = new PackedIntLists(2);

        int first = lists.list();
        lists.append(10);
        lists.append(20);
        lists.append(30);

        int second = lists.list();
        lists.append(40);

        assertEquals(0, first);
        assertEquals(3, lists.size(first));
        assertEquals(10, lists.get(first, 0));
        assertEquals(20, lists.get(first, 1));
        assertEquals(30, lists.get(first, 2));
        assertEquals(4, second);
        assertEquals(1, lists.size(second));
        assertEquals(40, lists.get(second, 0));
    }

    @Test
    void buildsPackedListsFromLinkedListArrayForEach() {
        IntLinkedListArray linked = new IntLinkedListArray(2, 2);

        int sourceFirst = linked.list();
        linked.append(sourceFirst, 7);
        linked.append(sourceFirst, 8);

        int sourceSecond = linked.list();
        linked.append(sourceSecond, 9);

        PackedIntLists packed = new PackedIntLists(2);

        int packedFirst = packed.list();
        linked.forEach(sourceFirst, packed::append);

        int packedSecond = packed.list();
        linked.forEach(sourceSecond, packed::append);

        assertEquals(0, packedFirst);
        assertEquals(3, packedSecond);
        assertEquals(2, packed.size(packedFirst));
        assertEquals(7, packed.get(packedFirst, 0));
        assertEquals(8, packed.get(packedFirst, 1));
        assertEquals(1, packed.size(packedSecond));
        assertEquals(9, packed.get(packedSecond, 0));
    }
}
