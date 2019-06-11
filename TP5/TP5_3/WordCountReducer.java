package TP5_3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context sortie) throws IOException, InterruptedException {
		Configuration conf = sortie.getConfiguration();
		String delimiter = conf.get("separateur");
		Iterator<Text> it = values.iterator();
		ArrayList<ArrayList<String>> equipements = new ArrayList<ArrayList<String>>();
		// HashMap<String, ArrayList<String>> equipements = new HashMap<String, ArrayList<String>>();
		String value;
		String currentList="";
		String currentList2="";
		while (it.hasNext()) {
			value = it.next().toString();
			if (value.startsWith("1")) {
				value = value.substring(2);
				currentList+=value;
			}
			// equipement
			else if (value.startsWith("2")) {
				value = value.substring(2);
				currentList2+=value;				currentList2=currentList2.substring(1);
			}	
		}
		for (String equipement : currentList.split(delimiter)) {			if(equipement.length()>0){
				sortie.write(new Text(key), new Text(currentList2+delimiter+equipement));			}
		}
	}
}
