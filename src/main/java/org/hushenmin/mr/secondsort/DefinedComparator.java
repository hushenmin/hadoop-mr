package org.hushenmin.mr.secondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Administrator on 2018/2/7.
 */
public class DefinedComparator extends WritableComparator {
    protected DefinedComparator(){
        super(CombinationKey.class,true);

    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CombinationKey c1 = (CombinationKey)a;
        CombinationKey c2 = (CombinationKey)b;
        int minus = c1.getFirstKey().compareTo(c2.getFirstKey());
        if (minus != 0){
            return  minus;
        }else {
           return c1.getSecondKet() - c2.getSecondKet();
        }

    }



}
