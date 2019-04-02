package DBMS.memoryManager.util;

public class List<T> {

    private Node<T> head = null;
    private Node<T> tail = null;
    private int size = 0;

    
    public  Node<T> remove(Node<T> node) {
    	if (node == null) {
    		System.out.println("REMOVE null NODE");
    		return null;
    	}
    	
    	if (node == tail && node == head) {
			tail = head = null;
		} else if (node.getNext() != null && node.getPrev() != null) {
			node.getPrev().setNext(node.getNext());
			node.getNext().setPrev(node.getPrev());
		} else if (node == head) {
			head = node.getNext();
			head.setPrev(null);
		} else if (node == tail) {
			tail = node.getPrev();
			tail.setNext(null);
		}
	
        size--;
        node.setList(null);
        node.setNext(null);
        node.setPrev(null);
        return node;
    }
    
    
    
    public  Node<T> remove(T value) {
        Node<T> node = head;
        while (node != null && (!node.getValue().equals(value))) {
            node = node.getNext();
        }
        return remove(node);
    }
    
    public Node<T> add(T value) {
        Node<T> n = new Node<T>(value);
    	return add(n);
    }

    
    public Node<T> add(Node<T> node) {
    	node.setNext(null);
    	node.setPrev(null);
    	
        if (head == null) {
            head = node;
            tail = node;
        } else {
            tail.setNext(node);
            node.setPrev(tail);
            tail = node;
        }
        size++;
        node.setList(this);
        return node;
    }

    public void clear() {
        head = null;
        tail = null;
        size = 0;
    }

  
    public boolean contains(T value) {
        Node<T> node = head;
        while (node != null) {
            if (node.getValue().equals(value))
                return true;
            node = node.getNext();
        }
        return false;
    }

    public int size() {
        return size;
    }

    
    public String toString() {
        StringBuilder builder = new StringBuilder();
        Node<T> node = head;
        while (node != null) {
            builder.append(node.getValue()).append(", ");
            node = node.getNext();
        }
        return builder.toString();
    }



	public Node<T> getHead() {
		return head;
	}



	public void setHead(Node<T> head) {
		this.head = head;
	}



	public Node<T> getTail() {
		return tail;
	}



	public void setTail(Node<T> tail) {
		this.tail = tail;
	}

    

}