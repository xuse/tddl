package com.taobao.tddl.interact.rule.virtualnode;

public class PartitionFunction {

    private int[] count;
    private int[] length;

    private int firstValue;


    public void setFirstValue(int firstValue) {
        this.firstValue = firstValue;
    }


    public void setPartitionCount(String partitionCount) {
        this.count = this.toIntArray(partitionCount);
    }


    public void setPartitionLength(String partitionLength) {
        this.length = this.toIntArray(partitionLength);
    }


    public int[] getCount() {
        return count;
    }


    public int[] getLength() {
        return length;
    }


    public int getFirstValue() {
        return firstValue;
    }


    private int[] toIntArray(String string) {
        if (string == null || string.isEmpty()) {
            return null;
        }

        String[] strs = string.split(",");
        int[] ints = new int[strs.length];
        for (int i = 0; i < strs.length; ++i) {
            ints[i] = Integer.parseInt(strs[i]);
        }
        return ints;
    }
}
