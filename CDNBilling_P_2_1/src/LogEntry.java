import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class LogEntry implements WritableComparable<LogEntry>{
	private int status;
	private long length;
	
	public LogEntry() {
		
	}
	
	public LogEntry(int status, long length) {
		this.status = status;
		this.length = length;
	}
	
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public long getLength() {
		return length;
	}
	public void setLength(long length) {
		this.length = length;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		status = in.readInt();
		length = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {	
		out.writeInt(status);
		out.writeLong(length);
	}

	@Override
	public int compareTo(LogEntry arg0) {
		
		return 0;
	}	
}
