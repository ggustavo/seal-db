package DBMS.memoryManager.util;

public class Node<T> {

    private T value = null;
    private Node<T> prev = null;
    private Node<T> next = null;
    private List<T> list = null;
    
    public Node() { }

    public Node(T value) {
        this.value = value;
    }
    
    @Override
    public String toString() {
        return value.toString();
    }

	public List<T> getList() {
		return list;
	}

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}

	public Node<T> getPrev() {
		return prev;
	}

	public void setPrev(Node<T> prev) {
		this.prev = prev;
	}

	public Node<T> getNext() {
		return next;
	}

	public void setNext(Node<T> next) {
		this.next = next;
	}

	public void setList(List<T> list) {
		this.list = list;
	}
	
	

}