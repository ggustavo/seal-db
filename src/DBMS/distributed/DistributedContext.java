package DBMS.distributed;

public class DistributedContext {
	
	private ResourceManagerConnection node;

	public ResourceManagerConnection getNode() {
		return node;
	}

	public void setNode(ResourceManagerConnection node) {
		this.node = node;
	}
	
	
	public void stop() {
		node.unRegister();
	}
}
