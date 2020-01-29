package serv0;

import java.net.*;
import java.io.*;

public class Slave extends Thread {
	Socket sock;
	int nbmach;
	int taillefragment = 1000000; 
	String nomfichier ;
	String cmd;
	String ext;


	public Slave(Socket s,int nb) {
		this.sock = s;
		this.nbmach = nb;

	}

	public void run() {
		
		try {
						
            BufferedInputStream in = new BufferedInputStream(sock.getInputStream() );
  	BufferedOutputStream out = new BufferedOutputStream(sock.getOutputStream());
	    DataInputStream indata = new DataInputStream(sock.getInputStream() );
            

	    int leng = 0;

     		 if ((leng = indata.readInt()) > -1) {
        	        System.out.println("Taille : " + leng);
		}
	   else {

                System.out.println("Taille non recu ");
	}
             byte[] byteArray = new byte[leng];
	     int byteRead = 0;

            if ((byteRead = in.read(byteArray)) != -1) {
                String nometcmd  = new String(byteArray);
                String[] tabl = nometcmd.split(" ");
                this.cmd = tabl[1];
                if (tabl[0].indexOf('.') > 0 ) {
                String[] tabl1 = (tabl[0]).split("\\.");
                this.nomfichier = tabl1[0];
                this.ext = tabl1[1];
                }
                else {
                this.nomfichier = tabl[0];
                this.ext = "";
                } 
        //        System.out.println("Nom du fichier est : " + nomfichier + " ext : "+ext);
                System.out.println("La commande est : " + cmd);

            	if (cmd.contains("CMDWRITE") ) {
// indata = new DataInputStream(sock.getInputStream());
            	  	

            		

            	  	
           	     	//byteRead = 0;
           	     	int numfrag = 0;
		byte[] tfrag = new byte[1];
		if ((in.read(tfrag)) != -1) {
				int longu = (int) tfrag[0];

				byte[] b = new byte[longu];
				if ((in.read(b)) != -1) {
                			String t  = new String(b);

					numfrag = Integer.parseInt(t.trim());
         	       			System.out.println("Num du fragement recu : "+numfrag);
				}
		   		else {
	
            	       			System.out.println("Num du fragement non recu ");
				}
		

           	  	 }
           	  	 
           	  	 else {
            	       System.out.println("Num du fragement non recu ");
           	  	 }
            		
             	    FileOutputStream os = new FileOutputStream(nomfichier+numfrag+"."+ext);

              //	    System.out.println("Fragment num "+numfrag+" créé");

		    int taillefrag = 0;
		tfrag = new byte[1];
		if ((in.read(tfrag)) != -1) {
				int longu = (int) tfrag[0];

				byte[] b = new byte[longu];
				if ((in.read(b)) != -1) {
                			String t  = new String(b);

					taillefrag = Integer.parseInt(t.trim());
      		          		System.out.println("fragment recu de taille : " + taillefrag);
				}
		   		else {
	
            	  			  System.out.println("Taille non recu ");
				}
		}
		   else {

            	  	  System.out.println("Taille non recu ");
		}

              	    
		    int nbsousfrag = taillefrag/taillefragment; 
    		  //  System.out.println("nb soufrag : " + nbsousfrag);
    		    int sousfragrest = taillefrag % taillefragment;
    		  //  System.out.println("sousfragrest : " + sousfragrest);
    		    for (int i=0; i < nbsousfrag; i++) { 
			byteArray = new byte[taillefragment];
    		    	byteRead = 0;
    		  	System.out.println("num soufrag : " + i);
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


         /*     indata1.close();
			os.close();
            		in.close();
            sock.close(); */
            		in.close();
			out.close();
			indata.close();

		}



         	  if (cmd.contains("CMDDELETE") ) {
        //        DataInputStream indata1 = new DataInputStream(sock.getInputStream());
         	  	
        	     byteRead = 0;
        	     int numfrag = 0;
        	  	 byte[] tfrag = new byte[1];
		if ((in.read(tfrag)) != -1) {
				int longu = (int) tfrag[0];

				byte[] b = new byte[longu];
				if ((in.read(b)) != -1) {
                			String t  = new String(b);

					numfrag = Integer.parseInt(t.trim());
      	            System.out.println("Num du fragement recu : "+numfrag);
        	  	 }
        	  	 
        	  	 else {
         	       System.out.println("Num du fragement non recu ");
        	  	 }
        }else {
         	       System.out.println("Num du fragement non recu ");
        	  	 }
        	   
        	   File f = new File(nomfichier+numfrag+"."+ext);
        	   f.delete();
         	       System.out.println("fragement "+numfrag+" du fichier "+nomfichier+" supprimé");
            		in.close();
			out.close();
			indata.close();
           	}
           
           	if (cmd.contains("CMDREAD")) {
      	        System.out.println("CMDREAD ");


     	  	
           	/*     byteRead = 0;
           	     int nbfrag = 0;
           	  	 if((byteRead=indata.readInt())>0) {
           	  		nbfrag = (int) byteRead;
            	       System.out.println("Nombre de fragements recu : "+nbfrag);
           	  	 }
           	  	 else {
           	       System.out.println("Nombre de fragements non recu");
           	  	 }
         	int i = 0; */
         //	while(i<nbfrag) {
         		
         //	  DataInputStream indata1 = new DataInputStream(sock.getInputStream());
         	  	
        	     byteRead = 0;
        	     int numfrag = 0;
		byte[] tfrag = new byte[1];
        	  	 if ((in.read(tfrag)) != -1) {
				int longu = (int) tfrag[0];

				byte[] b = new byte[longu];
				if ((in.read(b)) != -1) {
                			String t  = new String(b);

					numfrag = Integer.parseInt(t.trim());
      	        		System.out.println("Num du fragement recu : "+numfrag);
				}
        	  	 
	        	  	 else {
		         	       System.out.println("Num du fragement non recu ");
	        	  	 }
        	  	 }
        	  	 
        	  	 else {
         	      	 System.out.println("Num du fragement non recu ");
        	  	 }
           	  	          // 	  	DataOutputStream outdata = new DataOutputStream(sock.getOutputStream());
           		File f = new File(nomfichier+numfrag+"."+ext);

           		FileInputStream raf = new FileInputStream(f);
           		long len = f.length();
           		int nbsousfrag = ((int)len)/taillefragment; 

    			int sousfragrest = ((int)len) % taillefragment;
    			
	byte[] b = Integer.toString((int)len).getBytes();
	byte longu = (byte) b.length;
    System.out.println("envoie de la taille");
	out.write(longu);
    out.flush();
	out.write(b);
    out.flush();
           	    	System.out.println("fragement de taille "+len+" sera envoyé");
    			System.out.println("nb soufrag : " + nbsousfrag);
    		for (int i=0; i < nbsousfrag; i++) {
           	    byte[] buf = new byte[(int) taillefragment];
           	    int val = raf.read(buf);
           	    if(val != -1) {

           	        out.write(buf);
           	        out.flush();
           	        System.out.println("fragement de taille "+taillefragment+" envoyé pour lecture");
           	    }
           	    else {
           	    	System.out.println("Error read");
           	    }
    		}
    		if (sousfragrest != 0)  {
    			byte[] buf = new byte[(int) sousfragrest];
           	    int val = raf.read(buf);
           	    if(val != -1) {

        	
      
           	        out.write(buf);
           	        out.flush();
           	    	System.out.println("frag de taille "+sousfragrest+" envoyé pour lecture");	
				}
			else {
		 		System.out.println("Error read");
	 	  	}
    		}
    		
        	   
        	   
           		}
	 /*  	else {
		 System.out.println("Commande non valide");
	 	  } */
		
            }
            
            

                

  /*
           
            os.close();
            in.close(); 
            indata1.close(); */
            		in.close();
			out.close();
			indata.close();
            	
            }


		

		catch (Exception e) {
			e.printStackTrace();

		}

	}

}
