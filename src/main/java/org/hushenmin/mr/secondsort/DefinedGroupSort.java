package org.hushenmin.mr.secondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Administrator on 2018/2/7.
 */
public class DefinedGroupSort extends WritableComparator {
    public DefinedGroupSort() {
        super(CombinationKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CombinationKey c1 = (CombinationKey) a;
        CombinationKey c2 = (CombinationKey) b;
        return  c1.getFirstKey().compareTo(((CombinationKey) b).getFirstKey());
    }
}
