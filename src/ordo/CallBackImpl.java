package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {
	private Semaphore mutex;

	public CallBackImpl() throws RemoteException {
		this.mutex = new Semaphore(0);
	}
	@Override
	public void inc() throws RemoteException{
		this.mutex.release();
	}
	@Override
	public void dec() throws RemoteException, InterruptedException {
		this.mutex.acquire();
	}
}
