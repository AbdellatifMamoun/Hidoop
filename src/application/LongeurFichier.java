package application;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;

public class LongeurFichier implements MapReduce {
	
	private static final long serialVersionUID = 1L;
	
	@Override
	public void map(FormatReader reader, FormatWriter writer) {
		KV kv;
		Integer i = 0;
		while ((kv = reader.read()) != null) {
            StringTokenizer st = new StringTokenizer(kv.v);
			i += st.countTokens();
        }
        writer.write(new KV("taille", i.toString()));
	}

    @Override
	public void reduce(FormatReader reader, FormatWriter writer) {
		KV kv;
        Integer i =0;
		while ((kv = reader.read()) != null) {
			StringTokenizer st = new StringTokenizer(kv.v);
            while (st.hasMoreTokens()) {
			    i += Integer.parseInt(st.nextToken());
            }
		}
		writer.write(new KV("taille",i.toString()));
	}
	
	public static void main(String[] args) {
		Job j = new Job();
        j.setInputFormat(Format.Type.LINE);
        j.setInputFname(args[0]);
       long t1 = System.currentTimeMillis();
		j.startJob(new LongeurFichier());
		long t2 = System.currentTimeMillis();
        System.out.println("time in ms ="+(t2-t1));
        System.exit(0);

	}
}
