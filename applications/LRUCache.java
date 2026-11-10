/**
@implementation of Least Recent Used cache
*/

package applications; 
import java.util.HashMap;
 
public class LRUCache {
	private HashMap<Integer, DoubleLinkedListNode> map 
		= new HashMap<Integer, DoubleLinkedListNode>();
	private DoubleLinkedListNode head;
	private DoubleLinkedListNode end;
	private int capacity;
	private int len;
 
	public LRUCache(int capacity) {
		this.capacity = capacity;
		len = 0;
	}
	
	public LRUCache(LRUCache another){
		this.map = another.map;
		this.head = another.head;
		this.end = another.end;
		this.capacity = another.capacity;
		this.len = another.len;
	}
 
	public String get(int key) {
		if (map.containsKey(key)) {
			DoubleLinkedListNode latest = map.get(key);
			removeNode(latest);
			setHead(latest);
			return latest.val;
		} else {
			return "";
		}
	}
	
	public void print_cache(){
		for(int key:map.keySet()){
			System.out.println("[" + map.get(key).key + "] = " + map.get(key).val);
		}
	}
 
	public void removeNode(DoubleLinkedListNode node) {
		DoubleLinkedListNode cur = node;
		DoubleLinkedListNode pre = cur.pre;
		DoubleLinkedListNode post = cur.next;
 
		if (pre != null) {
			pre.next = post;
		} else {
			head = post;
		}
 
		if (post != null) {
			post.pre = pre;
		} else {
			end = pre;
		}
	}
 
	public void setHead(DoubleLinkedListNode node) {
		node.next = head;
		node.pre = null;
		if (head != null) {
			head.pre = node;
		}
 
		head = node;
		if (end == null) {
			end = node;
		}
	}
 
	/**
	 * Behavior is byte-for-byte identical to the original void set(): same
	 * promote-on-update, same insert-under-capacity, same evict-the-tail-
	 * when-full logic. The only change is the return type/value - this now
	 * returns the key that was evicted to make room (or null if nothing was
	 * evicted, i.e. the key already existed and was just updated, or there
	 * was still free capacity). Existing callers that treated this as void
	 * still compile unchanged; this lets a caller that wants exact,
	 * non-approximate eviction accounting (e.g. for measuring cache
	 * poisoning) know precisely which id was displaced, without an extra
	 * get()/peek() call that would itself disturb LRU order.
	 */
	public Integer set(int key, String value) {
		if (map.containsKey(key)) {
			DoubleLinkedListNode oldNode = map.get(key);
			oldNode.val = value;
			removeNode(oldNode);
			setHead(oldNode);
			return null;
		} else {
			DoubleLinkedListNode newNode = 
				new DoubleLinkedListNode(key, value);
			if (len < capacity) {
				setHead(newNode);
				map.put(key, newNode);
				len++;
				return null;
			} else {
				Integer evictedKey = end.key;
				map.remove(end.key);
				end = end.pre;
				if (end != null) {
					end.next = null;
				}
 
				setHead(newNode);
				map.put(key, newNode);
				return evictedKey;
			}
		}
	}
	
	public int get_len(){
		return this.len;
	}
}
 
class DoubleLinkedListNode {
	public String val;
	public int key;
	public DoubleLinkedListNode pre;
	public DoubleLinkedListNode next;
 
	public DoubleLinkedListNode(int key, String value) {
		val = value;
		this.key = key;
	}
}
