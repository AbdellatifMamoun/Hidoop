package ordo;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.io.File;

import config.Project;
import formats.Format;
import formats.Format.OpenMode;
import formats.Format.Type;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.HdfsClient;
import map.MapReduce;
import map.Reducer;

public class Job implements JobInterfaceX {
	
	private int taskMap;
	private int taskReduce;
	private Format.Type inputFormat;
	private Format.Type outputFormat;
	private SortComparator sc;
	private String inName;
	private String outName;
	
    public void runReduce(Reducer r, Format reader, Format writer) {
		System.out.println();
		reader.open(OpenMode.R);
		writer.open(OpenMode.W);
		r.reduce(reader, writer); 
		reader.close();
		writer.close();
	}

	@Override
	public void setInputFormat(Format.Type ft) {
		this.inputFormat = ft;
	}

	@Override
    public void setInputFname(String fname) {
    	this.inName = fname;
    }

	@Override
	public void setNumberOfReduces(int tasks) {
		this.taskReduce = tasks;		
	}

	@Override
	public void setNumberOfMaps(int tasks) {
		this.taskMap = tasks;		
	}

	@Override
	public void setOutputFormat(Type ft) {
		this.outputFormat =ft;		
	}

	@Override
	public void setOutputFname(String fname) {
		this.outName = fname;		
	}

	@Override
	public void setSortComparator(SortComparator sc) {
		this.sc = sc;
	}

	@Override
	public int getNumberOfReduces() {
		return this.taskReduce;
	}

	@Override
	public int getNumberOfMaps() {
		return this.taskMap;
	}

	@Override
	public Type getInputFormat() {
		return this.inputFormat;
	}

	@Override
	public Type getOutputFormat() {
		return this.outputFormat;
	}

	@Override
	public String getInputFname() {
		return this.inName;
	}

	@Override
	public String getOutputFname() {
		return this.outName;
	}

	@Override
	public SortComparator getSortComparator() {
		return this.sc;
	}

	@Override
    public void startJob (MapReduce mr) {
		//
        String[] name = inName.split("\\.");
        String nom = name[0];
		int repFactor = 1;
		//
		ArrayList<Daemon> daemons = new ArrayList<Daemon>();
		ArrayList<String> inputFnames = new ArrayList<String>();
		//
		try {
			HdfsClient.HdfsWrite(this.inputFormat, inName, repFactor);
		} catch(Exception e) {
			
			e.printStackTrace();
		}
        int nombrefragements = HdfsClient.nombrefragements(inName);
        for(int i = 0; i<nombrefragements; i++) {
			inputFnames.add(i, "../Serv"+i+"/src/" + nom + i + ".txt" );
		}
		//
		setNumberOfReduces(1);
		//
		setNumberOfMaps(nombrefragements);
		//
        //Project.Nbmachines
		for(int i = 0; i<Project.NBHOSTS; i++) {
			try {
                //System.out.println(Project.HOSTS[i]+":"+Project.PORTS[i] + "/daemon");
				Daemon d = (Daemon)Naming.lookup("//"+Project.HOSTS[i]+":"+Project.PORTS[i] + "/daemon");

			//	Daemon d = (Daemon)Naming.lookup("//"+Project.HOSTS[i]+":"+Project.Portinit + "/daemon");
				daemons.add(d);
                
			} catch (MalformedURLException | RemoteException | NotBoundException e) {
				
				e.printStackTrace();
			}
				
		}
		//
		CallBack cb;
		//
		try {
			cb = new CallBackImpl();
			//
			for(int i = 0; i < getNumberOfMaps(); i++) {
				KVFormat writer = new KVFormat("../Serv"+(i%(Project.NBHOSTS))+"/src/" + "tmp" + i + ".txt");
				KVFormat kvReader = new KVFormat("../Serv"+(i%(Project.NBHOSTS))+"/src/" + nom + i + ".txt");
				LineFormat lineReader = new LineFormat("../Serv"+(i%(Project.NBHOSTS))+"/src/" + nom + i + ".txt");
                    
				if (getInputFormat() == Type.KV) {
					daemons.get(i).runMap(mr, kvReader, writer, cb);
                    				
				} else {
                    /*daemons.get(i);
                    Daemon d1 = new DaemonImpl();
                    d1.runMap(mr, lineReader, writer, cb); */
                    Daemon d1 = daemons.get(i%(Project.NBHOSTS));
                    d1.runMap(mr, lineReader, writer, cb);
                       System.out.println("erreur 4");
					/*daemons.get(i).runMap(mr, lineReader, writer, cb);
                       System.out.println("erreur 4");	*/			
                }
			}
			//
             
			for(int i = 0; i < getNumberOfMaps(); i++) {
                try {				
                    cb.dec();
                } catch (Exception e) {
  
                }
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}
       try {
            HdfsClient.ajouter(inName,"tmp.txt");

          //  HdfsClient.HdfsDelete(inName);
        } catch (Exception e){e.printStackTrace();}
		try {
            //LineFormat inter = new LineFormat("e");
            //inter.open(OpenMode.W);
		    HdfsClient.HdfsRead("tmp.txt", "inter-"+inName);
            HdfsClient.HdfsDelete("tmp.txt");
		    KVFormat reduceReader = new KVFormat("inter-" + inName);
		    KVFormat reduceWriter = new KVFormat(inName+"-res");
            runReduce(mr, reduceReader, reduceWriter);
            File f = new File("inter-" + inName); 
            f.delete();           
            /*reduceReader.open(OpenMode.R);
		    reduceWriter.open(OpenMode.W);
		    mr.reduce(reduceReader, reduceWriter);
		    reduceReader.close();
		    reduceWriter.close();*/
            System.out.println("reduce");     
        } catch (Exception e) {
            
        }		
        
	}
}
