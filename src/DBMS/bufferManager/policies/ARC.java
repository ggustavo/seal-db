package DBMS.bufferManager.policies;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.bufferManager.IPage;


public class ARC extends AbstractBufferPolicy {

	public ARC(Integer capacity) {
		super(capacity);
	}

	protected List<IPage> T1 = Collections.synchronizedList(new ArrayList<IPage>());
	protected List<IPage> T2 = Collections.synchronizedList(new ArrayList<IPage>());
	protected List<IPage> B1 = Collections.synchronizedList(new ArrayList<IPage>());
	protected List<IPage> B2 = Collections.synchronizedList(new ArrayList<IPage>());
	protected int POINTER = 0;

	int L1() {
		return B1.size() + T1.size();
	}

	int L2() {
		return B2.size() + T2.size();
	}

	void replace(List<IPage> list) {

		if ((T1.size() >= 1) && ((list == B2 && T1.size() == POINTER) || (T1.size() > POINTER))) { // if (|T1| ≥ 1) and
																									// ((x ∈ B2 and |T1|
																									// = p) or (|T1| >
																									// p))

			IPage r = T1.get(0);
			free(r);
			B1.add(r);
			// insert_LRU(B1, flush(remove_LRU(T1, T1->tail))); //then move the LRU page of
			// T1 to the top of B1 and remove it from the cache.

		} else { // else move the LRU page in T2 to the top of B2 and remove it from the cache
			IPage r = T2.get(0);
			free(r);
			B2.add(r);
			// insert_LRU(B2, flush(remove_LRU(T2, T2->tail)));
		}

	}

	protected void hit(IPage p) {
		super.hitCount++;
		p.addHitCount();
		if (policyListener != null)
			policyListener.hit(p);
	}

	public synchronized IPage find(String pageId) {

		super.numberOfOperation++;

		for (int i = 0; i < T1.size(); i++) {
			IPage page = T1.get(i);
			if (page != null && page.getPageId().equals(pageId)) {
				T1.remove(page);
				insertT2(page);
				hit(page);
				return page;
			}
		}

		for (int i = 0; i < T2.size(); i++) {
			IPage page = T2.get(i);
			if (page != null && page.getPageId().equals(pageId)) {
				T2.remove(page);
				insertT2(page);
				hit(page);
				return page;
			}
		}

		super.missCount++;

		return null;
	}

	public void insert(IPage p) {

		action(() -> {
			asserts();

			for (int i = 0; i < B1.size(); i++) {
				IPage page = B1.get(i);
				if (page != null && page.getPageId() == p.getPageId()) {

					int delta = B2.size() / B1.size();
					POINTER = Math.min(capacity, (POINTER + Math.max(delta, 1)));
					replace(B1); // REPLACE(p).
					B1.remove(page);
					alloc(p);
					insertT2(p);

					return;
				}
			}

			for (int i = 0; i < B2.size(); i++) {
				IPage page = B2.get(i);
				if (page != null && page.getPageId() == p.getPageId()) {

					int delta = B1.size() / B2.size();
					POINTER = Math.max(0, POINTER - Math.max(delta, 1));
					replace(B2);
					B2.remove(page);
					alloc(p);
					insertT2(p);
					return;
				}
			}

			// MISS -------------------

			if (B1.size() + T1.size() == capacity) { // case (i) |L1| = c: el

				if (T1.size() < capacity) { // If |T1| < c
					B1.remove(B1.get(0));
					replace(null); // REPLACE(p).

				} else { // else
					IPage r = T1.get(0);
					T1.remove(r);
					free(r); // delete LRU page of T1 and remove it from the cache.

				}
			}

			else if ((L1() < capacity) && ((L1() + L2()) >= capacity)) { // case (ii) |L1| < c and |L1|+ |L2| ≥ c:
				if (L1() + L2() == 2 * capacity) { // if |L1|+ |L2|= 2c
					B2.remove(B2.get(0));
				}
				replace(null);
			}

			alloc(p);
			T1.add(p);

			if (policyListener != null)
				policyListener.insert(p);
		});

	}

	public void insertT2(IPage x) {
		T2.add(x);
	}

	public void remove(IPage p) {
		action(() -> {

			if (!T1.remove(p)) {
				if (!T2.remove(p)) {
					Kernel.log(this.getClass(), "Page " + p.getPageId() + " to be removed not found", Level.SEVERE);
				}
			}
			free(p);
			if (policyListener != null)
				policyListener.remove(p);
			if (policyListener != null)
				policyListener.setLastRemoved(p);

		});

	}

	public String toString() {
		return getName();
	}

	@Override
	public String getName() {

		return "Adptative Replacement Cache (ARC)";
	}

	@Override
	public List<IPage> getPages() {

		List<IPage> newList = Collections.synchronizedList(new ArrayList<IPage>());

		action(() -> {

			newList.addAll(T1);
			newList.addAll(T2);
		});

		return newList;
	}

	@Override
	public void setPolicyListener(BufferPolicyListener listener) {
		policyListener = listener;

	}

	@Override
	protected void logicRemoveAll() {
		action(() -> {
			POINTER = 0;
			B1.clear();
			B2.clear();
			T1.clear();
			T2.clear();
		});
	}

	public void savePage() {
		SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd hh-mm");
		String date = dt.format(new Date());

		try {
			PrintWriter logRequests = new PrintWriter(new FileWriter(new File(date + " ARC-B1"  + ".pages"), true));
			for (IPage p : B1) {
				logRequests.println(p.getPageId() + " [" + p.getType() + "]");
			}
			logRequests.close();
			
			logRequests = new PrintWriter(new FileWriter(new File(date + " ARC-T1"  + ".pages"), true));
			for (IPage p : T1) {
				logRequests.println(p.getPageId() + " [" + p.getType() + "]");
			}
			logRequests.close();
			
			logRequests = new PrintWriter(new FileWriter(new File(date + " ARC-T2"  + ".pages"), true));
			for (IPage p : T2) {
				logRequests.println(p.getPageId() + " [" + p.getType() + "]");
			}
			logRequests.close();
			
			logRequests = new PrintWriter(new FileWriter(new File(date + " ARC-B2"  + ".pages"), true));
			for (IPage p : B2) {
				logRequests.println(p.getPageId() + " [" + p.getType() + "]");
			}
			logRequests.close();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public int getCurrentNumberOfPages() {
		return T1.size() + T2.size();
	}

	void asserts() {
		if ((T1.size() + T2.size()) > capacity) {
			System.out.println("<<<<<< (T1+T2) exceeded the capacity >>");
		}
		if (L1() + L2() > 2 * capacity) {
			System.out.println("<<<<<< (T1+T2+B1+B2) exceeded the capacity >>");
		}
		if (T1.size() + B1.size() > capacity) {
			System.out.println("<<<<<< (T1+B1) exceeded the capacity >>");
		}
		if (T2.size() + B2.size() > 2 * capacity) {
			System.out.println("<<<<<< (T2+B2) exceeded the capacity*2 >>");
		}
	}

}
