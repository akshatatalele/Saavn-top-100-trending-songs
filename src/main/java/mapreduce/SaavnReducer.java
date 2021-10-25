package mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SaavnReducer extends Reducer<Text, Text, Text, Text> {
	Text[] top100 = new Text[100];

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		/*
		 * For each date, count the streams for each songs and find top 100
		 */
		// ArrayWritable aw = new ArrayWritable(Text.class);
		TreeMap<String, Integer> treemap = new TreeMap<String, Integer>();
		int count = 0;

		for (Text string : values) {
			if (treemap.containsKey(string.toString().split(",")[0])) {
				// has value
				count = treemap.get(string.toString().split(",")[0]) + 1;
				treemap.put(string.toString().split(",")[0], count);
			} else {
				treemap.put(string.toString().split(",")[0], 1);
			}
		}

		List<Entry<String, Integer>> sorted = entriesSortedByValues(treemap);

		for (int i = 0; i < 100; i++) {
			top100[i] = new Text(sorted.get(i).toString().split("=")[0]);
		}

		// aw.set(top100);
		String dt = key.toString(); // Start date
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar c = Calendar.getInstance();
		try {
			c.setTime(sdf.parse(dt));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		c.add(Calendar.DATE, 1); // number of days to add
		dt = sdf.format(c.getTime()); // dt is now the new date

		context.write(new Text(dt), new Text(printStrings()));
	}

	public String printStrings() {
		String strings = "";
		for (int i = 0; i < top100.length; i++) {
			strings = strings + "\n" + top100[i].toString();
		}
		return strings;
	}

	static <K, V extends Comparable<? super V>> List<Entry<K, V>> entriesSortedByValues(Map<K, V> map) {

		List<Entry<K, V>> sortedEntries = new ArrayList<Entry<K, V>>(map.entrySet());

		Collections.sort(sortedEntries, new Comparator<Entry<K, V>>() {
			public int compare(Entry<K, V> e1, Entry<K, V> e2) {
				return e2.getValue().compareTo(e1.getValue());
			}
		});

		return sortedEntries;
	}
}
