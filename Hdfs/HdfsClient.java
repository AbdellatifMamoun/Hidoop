/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import java.lang.*;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.BufferedOutputStream ;
import java.io.BufferedInputStream ;
import java.io.DataOutputStream ;
import java.io.DataInputStream ;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.nio.file.Files;
import java.nio.file.Paths;
import config.Project;
import java.rmi.Naming;
import java.rmi.NotBoundException;


public class HdfsClient {
private Socket soc;
private static int[] ports = Project.PORTS;
private static Socket[] socs;
private static int portinit = Project.Portinit;
private static int nbmachines = Project.Nbmachines;
private static long taillefrag = Project.Taillefrag;
private static int taillesousfrag = Project.Taillesousfrag;
private static int nbligne = 0;
private static Namenode NameNode;
public enum cmd {CMD_DELETE,CMD_WRITE,CMD_READ};


public HdfsClient(Socket sc,int nbmachine) {
	this.soc = sc;
	this.socs  = new Socket[nbmachines];
}
    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }
	
    public static void initialisation() {

    	socs  = new Socket[nbmachines];

    } 
    
    public static void HdfsDelete(String hdfsFname) throws UnknownHostException, IOException { 
    	
        long tstart = System.currentTimeMillis();
        HashMap<Integer,String> resultats = NameNode.getFragments(hdfsFname);
        
	//    	System.out.println(resultats.toString());
        //Socket[] socsread  = new Socket[nbmachines];
     
        Set<Entry<Integer, String>> set = resultats.entrySet();
        Iterator<Entry<Integer, String>> it = set.iterator();
        Socket soc;
        while(it.hasNext()) {
             Map.Entry<Integer,String> next = (Map.Entry<Integer,String>)it.next();
             Map.Entry<Integer,String> entry = next;
             int port =  Integer.parseInt(entry.getValue());
             soc  = new Socket("localhost",port);
	  	    System.out.println("port : " + port);
             
            
             
        
    	//initialisation();
        //envoie de la commande
        //envoyercommande(hdfsFname," CMDREAD"); 
        
             int leng = (hdfsFname + " CMDDELETE").length();
         
             byte[] buf = (hdfsFname + " CMDDELETE").getBytes();
             int indice = 0; 
             indice = entry.getKey();
	  	    	System.out.println("indice : " + indice);

    		 DataOutputStream dw = new DataOutputStream(soc.getOutputStream());
             BufferedOutputStream bw = new BufferedOutputStream(soc.getOutputStream());
    		 dw.writeInt(leng);
             dw.flush();
             bw.write(buf);
             bw.flush();
             
        //     DataOutputStream  dw1 = new DataOutputStream(soc.getOutputStream());


	  	   // try { Thread.sleep(10);}catch (Exception e) {}	
            byte[] b = Integer.toString(indice).getBytes();
	        byte longu = (byte) b.length;
	        dw.write(longu);
            dw.flush();
	        dw.write(b);
            dw.flush();
       //      dw1.writeInt(indice);


       //      dw1.flush();

        }
        long tfinish = System.currentTimeMillis();
        System.out.println("time : " + (tfinish-tstart));
        NameNode.supprimer(hdfsFname);

        resultats = NameNode.getFragments(hdfsFname);
        
        NameNode.Afficher();
        

    }
	
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, 
     int repFactor) throws Exception {
    	
    	initialisation();
    	
        long tstart = System.currentTimeMillis();

    	int machutil = nbmachines;
    	
    	File myfile = new File(localFSSourceFname);
    	FileInputStream raf = new FileInputStream(myfile);

    	
       long sourceSize = myfile.length();

        System.out.println(sourceSize);
        long numSplits = sourceSize/taillefrag; 
        System.out.println(numSplits);
        long remainingBytes = sourceSize % taillefrag;

	    BufferedReader read = new BufferedReader(new FileReader(new File(localFSSourceFname)));
	    int compteur = 0;
	    while (read.readLine() != null) {
 		    compteur++;
	    }
	    //read.close(); 

        // la variable compteur contient le nombre de lignes.
	    read = new BufferedReader(new FileReader(new File(localFSSourceFname)));
        System.out.println(compteur);

        // la variable nblignes contient le nombre de lignes correspondant approximativement à la taille du fragement.
        nbligne = (int) ( (((long)compteur)*taillefrag)/sourceSize ); 
        System.out.println("nb lignes : "+ nbligne);


        // la variable machutil correspond au nombre des machines utilisé lors du fragementation.
        if (remainingBytes > 0) {
        	machutil =  (int) numSplits+1;
        }
        else {
        	machutil = (int) numSplits;
        }
        NameNode.Afficher();

        //envoie de la commande
        //envoyercommande(localFSSourceFname," CMDWRITE");
		
		HashMap<Integer,String> cataloguefich = new HashMap<Integer,String>();
		
		if (machutil > nbmachines) {
			envoyerfrags(nbmachines, raf,read,cataloguefich,0,localFSSourceFname);	
			//System.out.println( nbmachines +" frags "+" Envoyés");
			int machrest = machutil - nbmachines;
			int i = 1;
			while (machrest > 0) {
				int nbmach ;
				if (machrest < nbmachines) {
					nbmach = machrest;
				}
				else {
					nbmach = nbmachines;
				}
				envoyerfrags(nbmach, raf,read,cataloguefich,i,localFSSourceFname);
			//	System.out.println( i*nbmach +" frags "+" Envoyés");
				machrest = machrest - nbmachines;
				i = i +1;
			}
		}
		else {
        	envoyerfrags(machutil, raf,read,cataloguefich,0,localFSSourceFname);
		}
        long tfinish = System.currentTimeMillis();
        System.out.println("time : " + (tfinish-tstart));
		NameNode.Ajouter(localFSSourceFname,cataloguefich);
        NameNode.Afficher();



       // raf.close();        
    

 }

static void envoyercommande(String nom,String cmd) throws IOException {
	int leng = (nom + cmd).length();
	byte[] buf = (nom + cmd).getBytes();
		
	for(int destIx=0; destIx < nbmachines; destIx++) {

        	socs[destIx] = new Socket("localhost",ports[destIx]);
        	
		    DataOutputStream dw = new DataOutputStream(socs[destIx].getOutputStream());
        	BufferedOutputStream bw = new BufferedOutputStream(socs[destIx].getOutputStream());
		    dw.writeInt(leng);
        	dw.flush();
    		bw.write(buf);
        	bw.flush();
        
    	}
}

//envoie d'un fragement de taille numBytes
static void readWrite(FileInputStream raf,BufferedOutputStream bw, long numBytes) throws IOException {
    byte[] buf = new byte[(int) numBytes];
    int val = raf.read(buf);
    if(val != -1) {

        bw.write(buf);
        bw.flush();
    }
    else {
    	System.out.println("Error read");
    }
}

//envoie des fragements sous une suite des sous fragements de taille taillesousfrag
static void readWrites(FileInputStream raf,BufferedOutputStream bw, int numBytes,int indice) throws IOException {

 //   System.out.println("frag num "+ indice +" Envoyé");
    int nbsousfrag = numBytes/taillesousfrag; 
    System.out.println("nombre des sous fragements : " + nbsousfrag);
    int sousfragrest = numBytes % taillesousfrag;
    System.out.println(sousfragrest);
    for (int i=0; i < nbsousfrag; i++) { 
	    readWrite(raf,bw, taillesousfrag);
    	    System.out.println("sous frag num "+ i +" Envoyé");
	}
    if (sousfragrest != 0)  {
		readWrite(raf,bw, sousfragrest);	
	}

}


//envoie du taille et du contenue d'un fragement 
static void envoyerfrags(int machutil,FileInputStream raf,BufferedReader br,HashMap<Integer,String> cataloguefich,int j,String nom) throws IOException {
	int leng = (nom + " CMDWRITE").length();
	byte[] buf = (nom + " CMDWRITE").getBytes();
		
	for(int destIx=0; destIx < machutil; destIx++) {
        	socs[destIx] = new Socket("localhost",ports[destIx]);
        	
		    DataOutputStream dw = new DataOutputStream(socs[destIx].getOutputStream());
        	BufferedOutputStream bw = new BufferedOutputStream(socs[destIx].getOutputStream());
		dw.writeInt(leng);
        	dw.flush();
    		bw.write(buf);
        	bw.flush();
        
    //	}
	
    //envoie de la taille d'un fragement nbrebytes
 //   for(int destIx=0; destIx < machutil; destIx++) {
    	int numfrag = nbmachines*j + destIx;
	byte[] b = Integer.toString(numfrag).getBytes();
	byte longu = (byte) b.length;
	bw.write(longu);
        bw.flush();
	bw.write(b);
        bw.flush();
    	
    	
    	// dw = new DataOutputStream(socs[destIx].getOutputStream());
       // try { Thread.sleep(10);}catch (Exception e) {}	

    	cataloguefich.put(numfrag, Integer.toString( ports[destIx]));
	    System.out.println("num frag envoyé : " + numfrag);
	    String line;
	    int nbrebytes = 0;
    	for(int i=0; i < nbligne; i++) {
		    try {	
		    	nbrebytes = br.readLine().getBytes().length +"\n".getBytes().length + nbrebytes;
		    } catch(Exception e) {
			    break;
		    }
	    }
    //	DataOutputStream dw1 = new DataOutputStream(socs[destIx].getOutputStream());
    
	/*byte b = (byte)nbrebytes;
	nbrebytes = 300000000;
	Integer nbbytes = new Integer(nbrebytes);

	Byte b2 = new Byte(b);

	    System.out.println("cong : "+ (nbrebytes/256));
	    System.out.println("b : "+ b);
	int coeff = 0;	
	int i2 = (b & 0xFF);
	if (b<0) {
		coeff = ((nbrebytes/256)+1);

	}else {
		coeff = nbrebytes/256;
	} 
	int n = (256*coeff)+b ;*/

	 b = Integer.toString(nbrebytes).getBytes();
	 longu = (byte) b.length;

	bw.write(longu);
        bw.flush();
	bw.write(b);
        bw.flush();
	System.out.println("Taille frag envoyé : " + nbrebytes);

        //envoie du contenue d'un fragement
	//    BufferedOutputStream bw = new BufferedOutputStream(socs[destIx].getOutputStream());
        readWrites(raf, bw, nbrebytes,destIx);


    }

}


	
    public static void HdfsRead(String hdfsFname, String localFSDestFname) throws IOException {

        HashMap<Integer,String> resultats = NameNode.getFragments(hdfsFname);
        long tstart = System.currentTimeMillis();        
	    	System.out.println(resultats.toString());
        //Socket[] socsread  = new Socket[nbmachines];
     
        Set<Entry<Integer, String>> set = resultats.entrySet();
        Iterator<Entry<Integer, String>> it = set.iterator();
        
        /*le fichier de sortie */
    	File myfile = new File(localFSDestFname);
    	FileOutputStream res = new FileOutputStream(myfile);
             Socket soc  ;//= new Socket("localhost",portinit);  
        while(it.hasNext()) {
             Map.Entry<Integer,String> next = (Map.Entry<Integer,String>)it.next();
             Map.Entry<Integer,String> entry = next;
             int port =  Integer.parseInt(entry.getValue());

	    	System.out.println("port :" + port);
            soc  = new Socket("localhost",port);

             
            
             
        
    	//initialisation();
        //envoie de la commande
        //envoyercommande(hdfsFname," CMDREAD"); 
        
             int leng = (hdfsFname + " CMDREAD").length();
         
             byte[] buf = (hdfsFname + " CMDREAD").getBytes();
            // int indice = 0; 
            int indice = entry.getKey();
	  	 System.out.println("indice : " + indice);

    		 DataOutputStream dw = new DataOutputStream(soc.getOutputStream());
  //           BufferedOutputStream bw = new BufferedOutputStream(soc.getOutputStream());
    		 dw.writeInt(leng);
             dw.flush();
             dw.write(buf);
             dw.flush();
             
      //       DataOutputStream  dw1 = new DataOutputStream(soc.getOutputStream());


	  	 //   try { Thread.sleep(100);}catch (Exception e) {}
	byte[] b = Integer.toString(indice).getBytes();
	byte longueur = (byte) b.length;
	dw.write(longueur);
        dw.flush();
	dw.write(b);
        dw.flush();	
          //   dw1.writeInt(indice);
	  	   // 	System.out.println("indice écrit");

         //    dw1.flush();
	  	     System.out.println("fragement d'indice " + indice + " demandé");

         //	InputStream is = soc.getInputStream();
    	  	BufferedInputStream indata = new BufferedInputStream(soc.getInputStream());
    	  	
    	    int byteRead = 0;
   	  	    int len = 0;

		byte[] tfrag = new byte[1];
        	  	 if ((indata.read(tfrag)) != -1) {
				int longu = (int) tfrag[0];

				 b = new byte[longu];
				if ((indata.read(b)) != -1) {
                			String t  = new String(b);

					len = Integer.parseInt(t.trim());
           	    	System.out.println("fragement de taille "+len+" est attendue");
     			 	}
				

   	  	 	}
    	  	
    		/*int nbsousfrag = taillefrag/300; 
    		    System.out.println("nb soufrag : " + nbsousfrag);
    		    int sousfragrest = taillefrag % 300;
    		    System.out.println("sousfragrest : " + sousfragrest);
    		    for (int i=0; i < nbsousfrag; i++) { 
			byteArray = new byte[300];
    		    	byteRead = 0;
	    		if ((byteRead = in.read(byteArray)) != -1) {

              	       		os.write(byteArray);         
            		}
		    }
    	            if (sousfragrest != 0)  {
			byteArray = new byte[sousfragrest];
    		    	byteRead = 0;
	    		if ((byteRead = in.read(byteArray)) != -1) {

              	       		os.write(byteArray);         
            		}
		    }
		    
*/
   	  	    int nbsousfrag = len/taillesousfrag; 
   	  	    System.out.println("nb soufrag : " + nbsousfrag);
   	  	    int sousfragrest = len % taillesousfrag;
   	      System.out.println("sousfragrest : " + sousfragrest);
   	  	for (int i=0; i < nbsousfrag; i++) { 

   	  	    //byte[] byteArray = new byte[1];
   	  	  
   	  	     byte[] byteArray = new byte[taillesousfrag];
	  	    //try { Thread.sleep(1000);}catch (Exception e) {}	
   	  	    if((indata.read(byteArray))!=-1) {

              //  System.out.println(b.length);
   	  	    	res.write(byteArray);
   	  	    	System.out.println("sous fragment de taille "+taillesousfrag+" lu");
   	  	    }else {
   	  	    	System.out.println("sous fragment non lu");
            }
   	  	    //is.close();
   	  	}
   	 if (sousfragrest != 0)  {

	  	    //byte[] byteArray = new byte[1];
	  	  
	  	    b = new byte[sousfragrest];
	  	    //try { Thread.sleep(1000);}catch (Exception e) {}	
	  	    if((indata.read(b))!=-1) {
          //   System.out.println(b.length);
	  	    	res.write(b);
	  	    	System.out.println("sous fragment de taille "+sousfragrest+" lu");
	  	    } else {
   	  	    	System.out.println("sous fragment non lu");
            }
   		 
   	 }
   	// is.close();

    	}
        long tfinish = System.currentTimeMillis();
        System.out.println("time : " + (tfinish-tstart));
    //	res.close();
    }

       /* Map<Integer, String> map = new TreeMap<Integer, String>(resultats); 
        Set set = map.entrySet();
        Iterator it = set.iterator();
        while(it.hasNext()) {
             Map.Entry entry = (Map.Entry)it.next();
                
        System.out.println(entry.getKey() + ": "+entry.getValue());*/
       
    
    
	
    public static void main(String[] args) {

        try {
			NameNode = (Namenode) Naming.lookup("//localhost:8088/namenode");
            
			
            if (args.length<2) {usage(); return;}

            switch (args[0]) {
              case "read": HdfsRead(args[1],args[2]); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write": 
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],1);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    
    }

}
