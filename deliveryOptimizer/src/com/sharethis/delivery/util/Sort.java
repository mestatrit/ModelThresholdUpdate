package com.sharethis.delivery.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class Sort {
	static private int order = 1;

	public static <K, V> List<K> byValue(final Map<K, V> m, String orderBy) {
		order = (orderBy.equals("asc") ? 1 : -1);
		List<K> keys = new ArrayList<K>();
		if (m == null) {
			return keys;
		}
		keys.addAll(m.keySet());
		Collections.sort(keys, new Comparator<Object>() {
			@SuppressWarnings("unchecked")
			public int compare(Object o1, Object o2) {
				Object v1 = m.get(o1);
				Object v2 = m.get(o2);
				if (v1 == null) {
					return (v2 == null ? 0 : 1);
				} else if (v1 instanceof Comparable) {
					return order * ((Comparable<Object>) v1).compareTo(v2);
				} else {
					return 0;
				}
			}
		});
		return keys;
	}
	
	public static <K, V> List<K> byAttribute(final Collection<K> m, final Map<K, V> n, String orderBy) {
		order = (orderBy.equals("asc") ? 1 : -1);
		List<K> keys = new ArrayList<K>();
		if (m == null) {
			return keys;
		}
		keys.addAll(m);
		Collections.sort(keys, new Comparator<Object>() {
			@SuppressWarnings("unchecked")
			public int compare(Object o1, Object o2) {
				Object v1 = n.get(o1);
				Object v2 = n.get(o2);
				if (v1 == null) {
					return (v2 == null ? 0 : 1);
				} else if (v1 instanceof Comparable) {
					return order * ((Comparable<Object>) v1).compareTo(v2);
				} else {
					return 0;
				}
			}
		});
		return keys;
	}
}
