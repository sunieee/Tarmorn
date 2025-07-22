package tarmorn.algorithm;

import java.util.ArrayList;
import java.util.Random;

import tarmorn.Settings;
import tarmorn.data.Triple;
import tarmorn.data.TripleSet;
import tarmorn.structure.Path;
import tarmorn.structure.Rule;

/**
* This class is responsible for sampling grounded pathes.
*
*/
public class PathSampler {

	private TripleSet ts;
	private Random rand = new Random();
	
	
	public static void main(String[] args) {
		
		
		
	}
	
	
	public PathSampler(TripleSet ts) {
		this.ts = ts;
	}
	
	
	public Path samplePath(int steps, boolean cyclic) {
		return samplePath(steps, cyclic, null);
	}
	
	
	public Path samplePath(int steps, boolean cyclic, Triple chosenHeadTriple) {
		return samplePath(steps, cyclic, chosenHeadTriple, null);
	}
	
	public Path samplePath(int steps, boolean cyclic, Triple chosenHeadTriple, Rule ruleToBeExtended) {
		// System.out.println("sample path");
		// if (ruleToBeExtended == null) return null;
		String[] nodes = new String[1 + steps * 2];
		char[] markers = new char[steps];
		ArrayList<Triple> chosenTriples;
		if (Settings.SINGLE_RELATIONS != null) {
			int rdice = this.rand.nextInt(Settings.SINGLE_RELATIONS.length);
			String singleRelation = Settings.SINGLE_RELATIONS[rdice];
			chosenTriples = ts.getTriplesByRelation(singleRelation);
			if (chosenTriples.size() == 0) {
				System.err.println("chosen a SINGLE_RELATION=" + singleRelation + " that is not instantiated in the training data");
				System.exit(0);	
			}
		}
		else {
			chosenTriples = ts.getTriples();
		}
		Triple triple = null;
		if (chosenHeadTriple == null) {
			int dice = this.rand.nextInt(chosenTriples.size());
			triple = chosenTriples.get(dice);
		}
		else triple = chosenHeadTriple;

		// TODO hardcoded test to avoid reflexive relations in the head
		if (triple.h.equals(triple.t)) return null;
		double dice = this.rand.nextDouble();
		if (ruleToBeExtended != null) {
			if (ruleToBeExtended.isXRule()) dice = 1;
			if (ruleToBeExtended.isYRule()) dice = 0;
		}
		if (dice < 0.5) {
			markers[0] = '+';
			nodes[0] = triple.h;
			nodes[1] = triple.r;
			nodes[2] = triple.t;
		}
		else {
			markers[0] = '-';
			nodes[2] = triple.h;
			nodes[1] = triple.r;
			nodes[0] = triple.t;
		}
		
		// add next hop
		int index = 1;
		while (index < steps) {
			if (this.rand.nextDouble() < 0.5) {
				ArrayList<Triple> candidateTriples = ts.getTriplesByHead(nodes[index*2]);
				if (candidateTriples.size() == 0) return null;
				Triple nextTriple;
				if (cyclic && index + 1 == steps) {
					ArrayList<Triple> cyclicCandidateTriples = new ArrayList<>();
					for (Triple t : candidateTriples) {
						if (t.t.equals(nodes[0])) cyclicCandidateTriples.add(t);
					}
					if (cyclicCandidateTriples.size() == 0) return null;
					nextTriple = cyclicCandidateTriples.get(this.rand.nextInt(cyclicCandidateTriples.size()));
				}
				else {
					nextTriple = candidateTriples.get(this.rand.nextInt(candidateTriples.size()));
				}
				nodes[index*2+1] = nextTriple.r;
				nodes[index*2+2] = nextTriple.t;
				markers[index] = '+';
			}
			else {
				ArrayList<Triple> candidateTriples = ts.getTriplesByTail(nodes[index*2]);
				if (candidateTriples.size() == 0) return null;
				Triple nextTriple;
				if (cyclic && index + 1 == steps) {
					ArrayList<Triple> cyclicCandidateTriples = new ArrayList<>();
					for (Triple t : candidateTriples) {
						if (t.h.equals(nodes[0])) cyclicCandidateTriples.add(t);
					}
					if (cyclicCandidateTriples.size() == 0) return null;
					nextTriple = cyclicCandidateTriples.get(this.rand.nextInt(cyclicCandidateTriples.size()));
				}
				else {
					nextTriple = candidateTriples.get(this.rand.nextInt(candidateTriples.size()));
				}
				nodes[index*2+1] = nextTriple.r;
				nodes[index*2+2] = nextTriple.h;
				markers[index] = '-';
			}
			index++;
		}
		// if (steps == 1) { System.out.println("... arrived and here"); }
		Path p = new Path(nodes, markers);
		if (steps == 1) return p;
		if (!cyclic && p.isCyclic()) return null;
		// check if path is valid
		return p;
	}
	

	
}
