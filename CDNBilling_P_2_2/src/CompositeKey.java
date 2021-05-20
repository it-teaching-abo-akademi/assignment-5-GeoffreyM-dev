import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKey implements Writable, WritableComparable<CompositeKey> {

	private String ip;
	private int count;

	public CompositeKey() {
	}

	public CompositeKey(String ip, int count) {
		this.ip = ip;
		this.count = count;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(ip).append("\t").append(count)).toString();
	}

	public void readFields(DataInput dataInput) throws IOException {
		ip = WritableUtils.readString(dataInput);
		count = WritableUtils.readVInt(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, ip);
		WritableUtils.writeVInt(dataOutput, count);
	}

	public int compareTo(CompositeKey objKeyPair) {
		Integer obj = new Integer(objKeyPair.count);
		int result = obj.compareTo(count);
		if (0 == result) {
			result = ip.compareTo(objKeyPair.ip);
		}
		return result;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
}