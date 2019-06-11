package TP4_3;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class WordCountMapper extends Mapper<LongWritable,Text, Text, Text> {
	
	//private Logger logger = Logger.getLogger(WordCountMapper.class);

	public void map(LongWritable cle, Text valeur, Context sortie) throws IOException {
		if((cle.get() > 0) && (valeur.toString().length() > 0)){
			try {			
				String line = valeur.toString();
				String region = line.split(";")[1];
				//logger.error("Ceci est un log qualitatif");
				if(region.startsWith("Haute-Garonne")){
					String x = line.split(";")[196];
					String y = line.split(";")[197];
					if(!x.equals(null) && !y.equals(null) && x.length()>0 && y.length()>0){
						if(Double.parseDouble(y)<Double.parseDouble(x)){
							String v =x;
							x=y;
							y=v;
						}
						double xd=Double.parseDouble(x);
						double yd=Double.parseDouble(y);
						double distance=distance(1.450, 43.617, yd, xd);
						//logger.error("Distance = " + distance + " Region = " + region);
						sortie.write(new Text(region), new Text(""+distance));
					} 
				}	
				
			} 
			catch (Exception ee) {
				throw new RuntimeException(ee.getMessage());
			}
		}
	}
	
	
	
	
	public double distance(double lat_pos1, double lon_pos1, double lat_pos2, double lon_pos2){
		double a = Math.PI / 180;
		double lat1 = lat_pos1 * a;
		double lat2 = lat_pos2 * a;
		double lon1 = lon_pos1 * a;
		double lon2 = lon_pos2 * a;
		double t1 = Math.sin(lat1) * Math.sin(lat2);
		double t2 = Math.cos(lat1) * Math.cos(lat2);
		double t3 = Math.cos(lon1 - lon2);
		double t4 = t2 * t3;
		double t5 = t1 + t4;
		double rad_dist = Math.atan(-t5 / Math.sqrt(-t5 * t5 + 1)) + 2 * Math.atan(1);
		return (rad_dist * 3437.74677 * 1.1508) * 1.6093470878864446;
	}
	
}