package org.hushenmin.mr.secondsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2018/2/7.
 */
public class CombinationKey implements WritableComparable<CombinationKey> {
    private String firstKey ;
    private int secondKet;

    public CombinationKey() {
    }

    public CombinationKey(String firstKey, int secondKet) {
        this.firstKey = firstKey;
        this.secondKet = secondKet;
    }

    public String getFirstKey() {
        return firstKey;
    }

    public void setFirstKey(String firstKey) {
        this.firstKey = firstKey;
    }

    public int getSecondKet() {
        return secondKet;
    }

    public void setSecondKet(int secondKet) {
        this.secondKet = secondKet;
    }

    public int compareTo(CombinationKey o) {
        return this.firstKey.compareTo(o.getFirstKey());
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.getFirstKey());
        dataOutput.writeInt(this.getSecondKet());
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.firstKey = dataInput.readUTF();
        this.secondKet = dataInput.readInt();
    }
}
