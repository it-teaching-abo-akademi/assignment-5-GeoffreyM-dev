import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKey implements Writable, WritableComparable<CompositeKey> {

	private String word;
	private int count;

	public CompositeKey() {
	}

	public CompositeKey(String word, int count) {
		this.word = word;
		this.count = count;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(word).append("\t").append(count)).toString();
	}

	public void readFields(DataInput dataInput) throws IOException {
		word = WritableUtils.readString(dataInput);
		count = WritableUtils.readVInt(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, word);
		WritableUtils.writeVInt(dataOutput, count);
	}

	public int compareTo(CompositeKey objKeyPair) {
		//Integer obj = new Integer(count);
		//int result = obj.compareTo(objKeyPair.count);
		Integer obj = new Integer(objKeyPair.count);
		int result = obj.compareTo(count);
		if (0 == result) {
			result = word.compareTo(objKeyPair.word);
		}
		return result;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String deptNo) {
		this.word = deptNo;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
}