package tarmorn.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import tarmorn.Settings;

public class TripleSet {
	
	private ArrayList<Triple> triples;
	private Random rand;
	
	
	HashMap<String, ArrayList<Triple>> h2List;
	HashMap<String, ArrayList<Triple>> t2List;
	HashMap<String, ArrayList<Triple>> r2List;
	
	HashMap<String, HashMap<String, HashSet<String>>> h2r2tSet;
	// HashMap<String, HashMap<String, HashSet<String>>> headTail2RelationSet;
	HashMap<String, HashMap<String, HashSet<String>>> t2r2hSet;
	
	HashMap<String, HashMap<String, ArrayList<String>>> h2r2tList;
	HashMap<String, HashMap<String, ArrayList<String>>> t2r2hList;
	
	HashSet<String> frequentRelations = new HashSet<>();
	
	HashMap<String,  ArrayList<String>> r2hSample = new HashMap<>();
	HashMap<String,  ArrayList<String>> r2tSample = new HashMap<>();

	HashMap<String, Integer> relationCounter = new HashMap<>();

	
	public static void main(String[] args) {
		// TripleSet ts = new TripleSet("data/DB500/ftest.txt");
	}
	
	public TripleSet(String filepath) {
		this();
		this.readTriples(filepath, true);
		this.indexTriples();
		// needs to be called from outside this.setupListStructure();
	}
	
	public TripleSet(String filepath, boolean ignore4Plus) {
		this();
		this.readTriples(filepath, ignore4Plus);
		this.indexTriples();
	}
	
	public TripleSet() {
		
		this.rand = new Random();
		
		this.triples = new ArrayList<>();
		this.h2List = new HashMap<>();
		this.t2List = new HashMap<>();
		this.r2List = new HashMap<>();

		this.h2r2tSet = new HashMap<>();
		// this.headTail2RelationSet = new HashMap<String, HashMap<String, HashSet<String>>>();
		this.t2r2hSet = new HashMap<>();
		
		this.h2r2tList = new HashMap<>();
		this.t2r2hList = new HashMap<>();
	}
	
	public void addTripleSet(TripleSet ts) {
		for (Triple t : ts.triples) {
			this.addTriple(t);
		}
	}
	
	public void addTriples(ArrayList<Triple> triples) {
		for (Triple t : triples) {
			this.addTriple(t);
		}
	}
	
	

	
	// fix stuff here
	public void addTriple(Triple t) {
		if (this.isTrue(t)) {
			return;
		}
		else {
			if (!t.invalid) this.triples.add(t);
			//if (this.atriples.containsKey(t)) {
			//	this.atriples.remove(t);
			//}
			//else {
				if (!t.invalid) this.addTripleToIndex(t);
			//}
		}
	}
	
	
	private void indexTriples() {
		long tCounter = 0;
		long divisor = 10000;
		for (Triple t : triples) {
			tCounter++;
			if (tCounter % divisor == 0) {
				System.out.println("* indexed " + tCounter + " triples");
				divisor *= 2;
			}
			addTripleToIndex(t);
		}
		System.out.println("* set up index for " + this.r2List.keySet().size() + " relations, " + this.h2List.keySet().size() + " head entities, and " + this.t2List.keySet().size() + " tail entities" );
	}
	
	public void setupListStructure() {
		//DecimalFormat df = new DecimalFormat("0.00");
		//System.out.println("MEMORY REQUIRED (before setupListStructure): " + df.format(Runtime.getRuntime().totalMemory() / 1000000.0) + " MByte at " + System.currentTimeMillis());
		
		System.out.print("* set up list structure for randomized access searches uring rule learning ... ");
		
		// head -> relation -> tails
		for (String head : this.h2r2tSet.keySet()) {
			this.h2r2tList.put(head, new HashMap<>());
			for (String relation : this.h2r2tSet.get(head).keySet()) {
				if (this.h2r2tSet.get(head).get(relation).size() > 10) { 
					this.h2r2tList.get(head).put(relation, new ArrayList<String>(this.h2r2tSet.get(head).get(relation).size()));
					this.h2r2tList.get(head).get(relation).addAll(this.h2r2tSet.get(head).get(relation));
					sampleSubset(this.h2r2tList.get(head).get(relation));
				}
			}
		}
		// tail -> relation -> head
		for (String tail : this.t2r2hSet.keySet()) {
			this.t2r2hList.put(tail, new HashMap<>());
			for (String relation : this.t2r2hSet.get(tail).keySet()) {
				if (this.t2r2hSet.get(tail).get(relation).size() > 10) {
					this.t2r2hList.get(tail).put(relation, new ArrayList<String>(this.t2r2hSet.get(tail).get(relation).size()));
					this.t2r2hList.get(tail).get(relation).addAll(this.t2r2hSet.get(tail).get(relation));
					sampleSubset(this.t2r2hList.get(tail).get(relation));
					
				}
			}
		}
		System.out.println(" done");
		//System.out.println("MEMORY REQUIRED (after setupListStructure): " + df.format(Runtime.getRuntime().totalMemory() / 1000000.0) + " MByte at " + System.currentTimeMillis());
		
	}
	
	private void sampleSubset(ArrayList<String> list) {
		Collections.shuffle(list);
		while (list.size() > 5000) {
			list.remove(list.size()-1);
		}
		
		
	}
	
	private void addTripleToIndex(Triple triple) {
		String h = triple.h;
		String t = triple.t;
		String r = triple.r;
		// index head
		if (!this.h2List.containsKey(h)) this.h2List.put(h, new ArrayList<>());
		this.h2List.get(h).add(triple);
		// index tail
		if (!this.t2List.containsKey(t)) this.t2List.put(t, new ArrayList<>());
		this.t2List.get(t).add(triple);
		// index relation
		if (!this.r2List.containsKey(r)) this.r2List.put(r, new ArrayList<>());
		this.r2List.get(r).add(triple);
		// index head-relation => tail
		if(!this.h2r2tSet.containsKey(h)) this.h2r2tSet.put(h, new HashMap<>());
		if (!this.h2r2tSet.get(h).containsKey(r)) this.h2r2tSet.get(h).put(r, new HashSet<>());
		this.h2r2tSet.get(h).get(r).add(t);
		// index tail-relation => head
		if(!this.t2r2hSet.containsKey(t)) this.t2r2hSet.put(t, new HashMap<>());
		if (!this.t2r2hSet.get(t).containsKey(r)) this.t2r2hSet.get(t).put(r, new HashSet<>());
		this.t2r2hSet.get(t).get(r).add(h);
	}


	private void readTriples(String filepath, boolean ignore4Plus) {
		Path file = (new File(filepath)).toPath();
		// Charset charset = Charset.forName("US-ASCII");
		Charset charset = Charset.forName("UTF8");
		String line = null;
		long lineCounter = 0;
		String s;
		String r;
		String o;
		try (BufferedReader reader = Files.newBufferedReader(file, charset)) { 
			while ((line = reader.readLine()) != null) {
				
				// System.out.println(line);
				lineCounter++;
			
				//  if (lineCounter % 7 == 0) continue;
				if (lineCounter % 1000000 == 0) {
					System.out.println(">>> parsed " + lineCounter + " lines");
				}
				if (line.length() <= 2) continue;
				String[] token = line.split("\t");
				if (token.length < 3) token = line.split(" ");
				Triple t = null;
				if (Settings.SAFE_PREFIX_MODE) {
					s = (Settings.PREFIX_ENTITY + token[0]).intern();
					r = (Settings.PREFIX_RELATION + token[1]).intern();
					o = (Settings.PREFIX_ENTITY + token[2]).intern();
				}
				else {
					s = token[0].intern();
					r = token[1].intern();
					o = token[2].intern();
				}
				
				if (token.length == 3) t = new Triple(s, r, o);
				if (token.length != 3 && ignore4Plus) t = new Triple(s, r, o);
				if (token.length == 4 && !ignore4Plus) {
					if (token[3].equals(".")) t = new Triple(s, r, o);
					else {						
						System.err.println("could not parse line " + line);
						t = null;
					}
				}
				// VERY SPECIAL CASE FOR SAMUELS DATASET
				if (token.length == 5 && !ignore4Plus) {
					String subject = token[0];
					String relation = token[1];
					String object = token[2];
					subject = subject.replace(" ", "_");
					relation = relation.replace(" ", "_");
					object = object.replace(" ", "_");
					t = new Triple(subject, relation, object);
				}
				
				if (t == null) { }
				else {
					if (!t.invalid) this.triples.add(t);
					if (Settings.REWRITE_REFLEXIV && t.t.equals(Settings.REWRITE_REFLEXIV_TOKEN)) {
						Triple trev;
						trev = new Triple(t.t, t.r, t.h);
						if (!trev.invalid) this.triples.add(trev);
					}
				}	
			}
		}
		catch (IOException x) {
			System.err.format("IOException: %s%n", x);
			System.err.format("Error occured for line: " + line + " LINE END");
		}
		// Collections.shuffle(this.triples);
		System.out.println("* read " + this.triples.size() + " triples");
	}
	
	public ArrayList<Triple> getTriples() {
		return this.triples;
	}
	
	
	public ArrayList<Triple> getTriplesByHead(String head) {
		if (this.h2List.containsKey(head)) {
			return this.h2List.get(head);
		}
		else {
			return new ArrayList<>();
		}
	}
	
	public ArrayList<Triple> getTriplesByHeadNotTail(String headOrTail, boolean byHeadNotTail) {
		if (byHeadNotTail) return this.getTriplesByHead(headOrTail);
		else return this.getTriplesByTail(headOrTail);
	}
	
	public ArrayList<Triple> getNTriplesByHead(String head, int n) {
		if (this.h2List.containsKey(head)) {
			if (this.h2List.get(head).size() <= n) return this.h2List.get(head);
			else {
				ArrayList<Triple> chosen = new ArrayList<>();
				for (int i = 0; i < n; i++) {
					int index = this.rand.nextInt(this.h2List.get(head).size());
					chosen.add(this.h2List.get(head).get(index));
				}
				return chosen;
			}
		}
		else return new ArrayList<>();
	}
	

	
	
	
	public ArrayList<Triple> getTriplesByTail(String tail) {
		if (this.t2List.containsKey(tail)) {
			return this.t2List.get(tail);
		}
		else {
			return new ArrayList<>();
		}
	}
	
	public ArrayList<Triple> getNTriplesByTail(String tail, int n) {
		
		if (this.t2List.containsKey(tail)) {
			if (this.t2List.get(tail).size() <= n) return this.t2List.get(tail);
			else {
				ArrayList<Triple> chosen = new ArrayList<>();
				for (int i = 0; i < n; i++) {
					int index = this.rand.nextInt(this.t2List.get(tail).size());
					chosen.add(this.t2List.get(tail).get(index));
				}
				return chosen;
			}
		}
		else return new ArrayList<>();
	}
	
	
	public ArrayList<Triple> getTriplesByRelation(String relation) {
		if (this.r2List.containsKey(relation)) {
			return this.r2List.get(relation);
		}
		else {
			return new ArrayList<>();
		}
	}
	
	public Triple getRandomTripleByRelation(String relation) {
		if (this.r2List.containsKey(relation)) {
			return this.r2List.get(relation).get(this.rand.nextInt(this.r2List.get(relation).size()));
		}
		return null;
	}
	
	
	public ArrayList<String> getNRandomEntitiesByRelation(String relation, boolean ifHead, int n) {
		if (ifHead) {
			if (r2hSample.containsKey(relation)) return r2hSample.get(relation);
		}
		else {
			if (r2tSample.containsKey(relation)) return r2tSample.get(relation);
		}
		return computeNRandomEntitiesByRelation(relation, ifHead, n);
	}
	
	
	public void precomputeNRandomEntitiesPerRelation(int n) {
		System.out.print("* precomputing random starting points for each relation/direction for the beam search ...");
		for (String relation : this.getRelations()) {
			this.computeNRandomEntitiesByRelation(relation, true, n);
			this.computeNRandomEntitiesByRelation(relation, false, n);
		}
	}
	
	
	private synchronized ArrayList<String> computeNRandomEntitiesByRelation(String relation, boolean ifHead, int n) {	
		if (this.r2List.containsKey(relation)) {
			ArrayList<String> entities = new ArrayList<>();
			HashSet<String> entitiesAsSet = new HashSet<>();
			for (Triple triple : this.r2List.get(relation)) {
				String value = triple.getValue(ifHead);
				if (!entitiesAsSet.contains(value)) {
					entitiesAsSet.add(value);
					entities.add(value);
				}
			}
			ArrayList<String> sampledEntities = new ArrayList<>();
			for (int i = 0; i < n; i++) {
				String entity = entities.get(rand.nextInt(entities.size()));
				sampledEntities.add(entity);
			}
			if (ifHead) this.r2hSample.put(relation, sampledEntities);
			else this.r2tSample.put(relation, sampledEntities);
			return sampledEntities;
		}
		else {
			System.err.println("something is strange, internal reference to relation " + relation + ", which is not indexed");
			System.err.println("check if rule set and triple set fit together");
			return null;
		}
	}
	
	/**
	 * Select randomly n entities that appear in head (or tail) position of a triple using a given relation.
	 * More frequent entities appear more frequent. This is the difference compared to the method computeNRandomEntitiesByRelation.
	 * 
	 * @param relation
	 * @param ifHead
	 * @param n
	 * @return
	 */
	public ArrayList<String> selectNRandomEntitiesByRelation(String relation, boolean ifHead, int n) {
		
		if (this.r2List.containsKey(relation)) {
			ArrayList<String> entities = new ArrayList<>();
			int j = 0;
			for (Triple triple : this.r2List.get(relation)) {
				j++;
				String value = triple.getValue(ifHead);
				entities.add(value);
				if (j == n) break;
			}
			ArrayList<String> sampledEntities = new ArrayList<>();
			for (int i = 0; i < n; i++) {
				String entity = entities.get(rand.nextInt(entities.size()));
				sampledEntities.add(entity);
			}
			return sampledEntities;
		}
		else {
			System.err.println("something is strange, internal reference to relation " + relation + ", which is not indexed");
			System.err.println("check if rule set and triple set fit together");
			return null;
		}
	}
	
	
	public Set<String> getRelations() {
		return this.r2List.keySet();
	}
	
	public Set<String> getHeadEntities(String relation, String tail) {
		if (t2r2hSet.get(tail) != null) {
			if (t2r2hSet.get(tail).get(relation) != null) {
				return t2r2hSet.get(tail).get(relation);
			}
		}
		return new HashSet<>();
	}
	
	public Set<String> getTailEntities(String relation, String head) {
		if (h2r2tSet.get(head) != null) {
			if (h2r2tSet.get(head).get(relation) != null) {
				return h2r2tSet.get(head).get(relation);
			}
		}
		return new HashSet<>();
	}
	
	/**
	* Returns those values for which the relation holds for a given value. If the ifHead is 
	* set to true, the value is interpreted as head value and the corresponding tails are returned.
	* Otherwise, the corresponding heads are returned.
	*  
	* @param relation The specified relation.
	* @param value The value interpreted as given head or tail.
	* @param ifHead Whether to interpret the value as head and not as tail (false interprets as tail).
	* @return The resulting values.
	*/
	public Set<String> getEntities(String relation, String value, boolean ifHead) {
		if (ifHead) return this.getTailEntities(relation, value);
		else return this.getHeadEntities(relation, value);
		
	}
	
	/**
	* Returns a random value for which the relation holds for a given value. If the ifHead is 
	* set to true, the value is interpreted as head value and the corresponding tails are returned.
	* Otherwise, the corresponding heads are returned.
	*  
	* @param relation The specified relation.
	* @param value The value interpreted as given head or tail.
	* @param ifHead Whether to interpret the value as head and not as tail (false interprets as tail).
	* @return The resulting value or null if no such value exists.
	*/
	public String getRandomEntity(String relation, String value, boolean ifHead) {
		if (ifHead) return this.getRandomTailEntity(relation, value);
		else return this.getRandomHeadEntity(relation, value);
		
	}
	
	private String getRandomHeadEntity(String relation, String tail) {
		if (!t2r2hList.containsKey(tail)) return null;
		ArrayList<String> list = this.t2r2hList.get(tail).get(relation);
		if (list == null)
			if (this.t2r2hSet.get(tail).get(relation) != null && this.t2r2hSet.get(tail).get(relation).size() > 0) {
				list = new ArrayList<>();
				list.addAll(this.t2r2hSet.get(tail).get(relation));
			}
			else {
				return null;
			}
		return list.get(this.rand.nextInt(list.size()));
	}
	
	private String getRandomTailEntity(String relation, String head) {
		if (!h2r2tList.containsKey(head)) return null;
		ArrayList<String> list = this.h2r2tList.get(head).get(relation);
		if (list == null) {
			if (this.h2r2tSet.get(head).get(relation) != null && this.h2r2tSet.get(head).get(relation).size() > 0) {
				list = new ArrayList<>();
				list.addAll(this.h2r2tSet.get(head).get(relation));
			}
			else {
				return null;
			}
		}
		return list.get(this.rand.nextInt(list.size()));
	}
	
	/*
	public Set<String> getRelations(String head, String tail) {
		if (headTail2RelationSet.get(head) != null) {
			if (headTail2RelationSet.get(head).get(tail) != null) {
				return headTail2RelationSet.get(head).get(tail);
			}
		}
		return new HashSet<String>();
	}
	*/
	
	public boolean isTrue(String head, String relation, String tail) {
		if (t2r2hSet.get(tail) != null) {
			if (t2r2hSet.get(tail).get(relation) != null) {
				return t2r2hSet.get(tail).get(relation).contains(head);
			}
		}
		return false;	
	}
	
	
	
	public boolean isTrue(Triple triple) {
		return this.isTrue(triple.h, triple.r, triple.t);
	}
	


	public void compareTo(TripleSet that, String thisId, String thatId) {
		System.out.println("* Comparing two triple sets");
		int counter = 0;
		for (Triple t : triples) {
			if (that.isTrue(t)) {
				counter++;
			}
		}
		
		System.out.println("* size of " + thisId + ": " +  this.triples.size());
		System.out.println("* size of " + thatId + ": " +  that.triples.size());
		System.out.println("* size of intersection: " + counter);
		
	}

	public TripleSet getIntersectionWith(TripleSet that) {
		TripleSet ts = new TripleSet(); 
		for (Triple t : triples) {
			if (that.isTrue(t)) {
				ts.addTriple(t);
			}
		}
		return ts;
	}

	public TripleSet minus(TripleSet that) {
		TripleSet ts = new TripleSet(); 
		for (Triple t : triples) {
			if (!that.isTrue(t)) {
				ts.addTriple(t);
			}
		}
		return ts;
	}

	public int getNumOfEntities() {
		return h2List.keySet().size() + t2List.keySet().size();
	}

	public void determineFrequentRelations(double coverage) {
		int allCounter = 0;
		for (Triple t : this.triples) {
			allCounter++;
			String r = t.r;
			if (relationCounter.containsKey(r)) {
				relationCounter.put(r, relationCounter.get(r) + 1);
			} else {
				relationCounter.put(r, 1);
			}
		}
		
		ArrayList<Integer> counts = new ArrayList<Integer>();
		counts.addAll(relationCounter.values());
		Collections.sort(counts);
		int countUp = 0;
		int border = 0;
		for (Integer c : counts) {
			countUp = countUp + c;
			//System.out.println("countUp: " + countUp);
			//System.out.println("c: " + c);
			if (((double)(allCounter - countUp) / (double)allCounter) < coverage) {
				border = c;
				break;
			}
		}
		
		//System.out.println("Number of all relations: " + relationCounter.size());
		//System.out.println("Relations covering " + coverage + " of all triples");
		for (String r : relationCounter.keySet()) {
			
			if (relationCounter.get(r) > border) {
				frequentRelations.add(r);
				//System.out.println(r + " (used in " + relationCounter.get(r) + " triples)");
			}
		}
		//System.out.println("Number of frequent (covering " + coverage+ " of all) relations: " + frequentRelations.size());
	}

	public boolean isFrequentRelation(String relation) {
		return this.frequentRelations.contains(relation);
	}

	/*
	public boolean existsPath(String x, String y, int pathLength) {
		if (pathLength == 1) {
			if (this.getRelations(x, y).size() > 0) {
				return true;
			}
			if (this.getRelations(y, x).size() > 0) {
				return true;
			}
			return false;
		}
		if (pathLength == 2) {
			Set<String> hop1x = new HashSet<String>();
			for (Triple hx : this.getTriplesByHead(x)) { hop1x.add(hx.getTail()); }
			for (Triple tx : this.getTriplesByTail(x)) { hop1x.add(tx.getHead()); }

			for (Triple hy : this.getTriplesByHead(y)) {
				if (hop1x.contains(hy.getTail())) return true;
			}
			for (Triple ty : this.getTriplesByTail(y)) {
				if (hop1x.contains(ty.getHead()))  return true;
			}
			return false;
		}
		if (pathLength > 2 ) {
			System.err.println("checking the existence of a path longer than 2 is so far not supported");
			System.exit(-1);
			
		}
		return false;
		
	}
	*/

	public Set<String> getEntities() {
		HashSet<String> entities = new HashSet<String>();
		entities.addAll(h2List.keySet());
		entities.addAll(t2List.keySet());
		return entities;
	}
	
	public void write(String filepath) throws FileNotFoundException {
		PrintWriter  pw = new PrintWriter(filepath);
		
		for (Triple t : this.triples) {
			 pw.println(t);
		}
		pw.flush();
		pw.close();
		
	}
	
	public int size() {
		return this.triples.size();
	}


	
	
	
	
	
}
