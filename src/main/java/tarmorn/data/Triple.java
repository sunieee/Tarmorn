package tarmorn.data;

import tarmorn.Settings;

/**
 * A triple represents a labeled edge a knowledge graph.
 * 
 *
 */
public class Triple {
	
	public boolean invalid = false;

	public final String h; // subject
	public final String t; // object
	public final String r;
	
	private int hash = 0;
	
	public Triple(String h, String r, String t) {
		if (h.length() < 2 || t.length() < 2) {
			System.err.println("the triple set you are trying to load contains constants of length 1 ... a constant (entity) needs to be described by at least two letters");
			System.err.println("ignoring: " + h + " " + r + " " + t);
			// System.exit(1);
			this.invalid = true;
		}
		this.h = h;
		this.r = r;
		if (Settings.REWRITE_REFLEXIV && h.equals(t)) {
			this.t = Settings.REWRITE_REFLEXIV_TOKEN;
		}
		else {
			this.t = t;
		}
		hash = this.h.hashCode() + this.t.hashCode() + this.r.hashCode();
	}
	
	public static Triple createTriple(String h, String r, String t, boolean reverse) {
		if (reverse) {
			return new Triple(t, r, h);
		}
		else {
			return new Triple(h, r, t);
		}
	}
	
	public String getValue(boolean ifHead) {
		if (ifHead) return this.h;
		else return this.t;
	}

	@Override
	public String toString() {
		return this.h + " " + this.r + " " + this.t;
	}
	
	@Override
	public boolean equals(Object that) {
		if (that instanceof Triple) {
			Triple thatTriple = (Triple)that;
			if (this.h.equals(thatTriple.h) && this.t.equals(thatTriple.t) && this.r.equals(thatTriple.r)) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return hash;
	}

	public boolean equals(boolean ifHead, String subject, String rel, String object) {
		if (ifHead) {
			return (this.h.equals(subject) && this.t.equals(object) && this.r.equals(rel)); 
		}
		else {
			return (this.h.equals(object) && this.t.equals(subject) && this.r.equals(rel)); 
		}
	}
	
	public double getConfidence() {
		return 1.0;
	}

	/**
	 * Returns a string representation of this triples by replacing the constant by a variable wherever it appears
	 * 
	 * @param constant The constant to be replaced.
	 * @param variable The variable that is shown instead of the constant.
	 * 
	 * @return The footprint of a triples that can be compared by equals against the atom in a AC1 rule.
	 */
	public String getSubstitution(String constant, String variable) {
		String tSub = this.t.equals(constant) ? variable : this.t;
		String hSub = this.h.equals(constant) ? variable : this.h;
		return this.r + "(" + hSub + "," + tSub + ")";
	}
	
	
	/**
	 * Returns a string representation of this triples by replacing the constant by a variable wherever it appears and repalcing the other constant by a sedocn variable.
	 * 
	 * @param constant The constant to be replaced.
	 * @param variable The variable that is shown instead of the constant.
	 * @param otherVariable The variable that is shown instead of the other constant.
	 * 
	 * @return The footprint of a triples that can be compared by equals against the atom in a AC2 rule .
	 */
	public String getSubstitution(String constant, String variable, String otherVariable) {
		String tSub = this.t.equals(constant) ? variable : this.t;
		String hSub = this.h.equals(constant) ? variable : this.h;
		if (tSub.equals(variable)) hSub = otherVariable;
		if (hSub.equals(variable)) tSub = otherVariable;
		return this.r + "(" + hSub + "," + tSub + ")";
	}


 	

}
