import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TwitterFields implements WritableComparable <TwitterFields>{

	private LongWritable epoch_time;
	private LongWritable tweetId;

	private Text tweet;
	private Text device;


	public TwitterFields() {
		set(new LongWritable(), new LongWritable(), new Text(), new Text());
	}


	public TwitterFields(long epoch_time, long tweetId, String tweet, String device) {
		set(new LongWritable(epoch_time), new LongWritable(tweetId), new Text(tweet), new Text(device));
	}

	public TwitterFields(LongWritable epoch_time , LongWritable tweetId, Text tweet, Text device) {
		set(epoch_time,tweetId,tweet,device);
	}

	public void set(LongWritable epoch_time, LongWritable tweetId, Text tweet, Text device) {
		this.epoch_time = epoch_time;
		this.tweetId = tweetId;
		this.tweet = tweet;
		this.device = device;

	}

	public LongWritable getEpoch_time() {
		return epoch_time;
	}


	public LongWritable getTweetId() {
		return tweetId;
	}


	public Text getTweet() {
		return tweet;
	}


	public Text getDevice() {
		return device;
	}
	// shifting the time according to timezone.. (from Greenwich to Brazial time)
	public int getTwitterHour () {
		LocalDateTime d = LocalDateTime.ofEpochSecond(this.getEpoch_time().get()/1000, 0, ZoneOffset.of("-3"));

		return d.getHour();

	}
	//readFields, reads the data (from network)
	@Override
	public void readFields(DataInput in) throws IOException {
		epoch_time.readFields(in);
		tweetId.readFields(in);
		tweet.readFields(in);
		device.readFields(in);

	}
	// write will write the data into local disk
	@Override
	public void write(DataOutput out) throws IOException {
		epoch_time.write(out);
		tweetId.write(out);
		tweet.write(out);
		device.write(out);

	}

	@Override
	public int compareTo(TwitterFields tf) {
		int cmp =  this.getTwitterHour() > tf.getTwitterHour() ? +1 : this.getTwitterHour() < tf.getTwitterHour() ? -1 : 0 ;

		return cmp;
	}
}
