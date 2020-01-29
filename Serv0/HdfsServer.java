
package serv0;
import java.io.IOException;
import java.net.*;


import formats.*;
/*
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat; */

public class HdfsServer {
	static int portclient ; //port de communication avec le HdfsClient de la même machine
	int[] portserveurs;  //liste des ports des autres Hdfs Serveurs
	int nbmachines = 5;
    public ServerSocket sSocket ;
    public String IP = "localhost";
    private boolean isRunning = true;
    
    public HdfsServer() {
        try {
        	portclient = 2020;
        	sSocket = new ServerSocket(portclient,1,InetAddress.getByName(IP));
        	//sSocket.open();
        	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    //On lance notre serveur
    public void open(){
       
       //Toujours dans un thread à part vu qu'il est dans une boucle infinie
       Thread t = new Thread(new Runnable(){
          public void run(){
             while(isRunning == true){
                
                try {
                   //On attend une connexion d'un client
                   Socket client = sSocket.accept();
                   
                   //Une fois reçue, on la traite dans un thread séparé
                   System.out.println("Connexion cliente reçue.");
                   Slave s = new Slave(client,nbmachines);
              //     Thread t = new Thread(new ClientProcessor(client));
                   s.start();
                   
                } catch (IOException e) {
                   e.printStackTrace();
                }
             }
             
             try {
                sSocket.close();
            	System.out.println(" Socket Serveur fermé");
             } catch (IOException e) {
                e.printStackTrace();
                sSocket = null;
             }
          }
       });
       
       t.start();
    }
    
    public void close(){
       isRunning = false;
    }   
    
    public static void main(String[] args) {
    	HdfsServer serv = new HdfsServer();
    	serv.open();

    }
 }

