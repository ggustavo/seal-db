package DBMS.connectionManager;

public interface ResponseMenssageListener<T> {
	
	void onReceiver(T o);
	void onErro(String e);
	Class<T> responseDataClass();
}
