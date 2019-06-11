package TP5_1;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WordCountMapper extends Mapper<LongWritable,Text, Text, Text> {
	
	public void map(LongWritable cle, Text valeur, Context sortie) throws IOException, InterruptedException {
		String nomFichier=((FileSplit) sortie.getInputSplit()).getPath().getName();
			if(nomFichier.equals("20140410_RES_FichesEquipementsActivites.csv")){
				if((cle.get() > 0) && (valeur.toString().length() > 0)){	
					try{			
					String line = valeur.toString();
					String idEquipement = line.split(";")[4];
					String libelleActivite=line.split(";")[7];
					sortie.write(new Text(idEquipement), new Text("1 "+libelleActivite));
					}catch(ArrayIndexOutOfBoundsException e){
					}
				} 
			}else{
				if(nomFichier.equals("20140410_RES_FichesEquipements.csv")){
					if((cle.get() > 0) && (valeur.toString().length() > 0)){			
						String line = valeur.toString();
						String idEquipement = line.split(";")[6];
						String libelleEquipement=line.split(";")[7];
						sortie.write(new Text(idEquipement), new Text("2 "+libelleEquipement));
					} 
				}
			}
		
		}	
	}
	
	
	
	
	
	