package tarmorn;

import org.yaml.snakeyaml.Yaml;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import tarmorn.data.TripleSet;
import tarmorn.io.IOHelper;
import tarmorn.structure.Dice;
import tarmorn.structure.Rule;
import tarmorn.threads.RuleWriterAsThread;
import tarmorn.threads.Scorer;

public class Learn {
	
	private static long timeStamp = 0;
	// used at the begging to check if all threads are really available
	private static HashSet<Integer> availableThreads = new HashSet<Integer>();
	
	private static RuleWriterAsThread rwt = null;
	
	/*
	 * Lets hope that people will not run AnyBURl with more than 100 cores ... up to these 307 buckets should be sufficient
	 * I somehow like the number 307
	 */
	@SuppressWarnings("unchecked")
	private static HashSet<Rule>[] rules307 = new HashSet[307];
	
	static {
		for (int i = 0; i < 307; i++) {
			HashSet<Rule> anonymRuleSet = new HashSet<Rule>(); 
			rules307[i] = new HashSet<Rule>(Collections.synchronizedSet(anonymRuleSet));
		}
	}
	
	public static int[][] stats;
	
	public static Dice dice;
	
	
	public static boolean active = true;
	public static boolean report = false;
	public static boolean[] activeThread;
	
	public static boolean finished = false;


	public static void main(String[] args) throws FileNotFoundException, InterruptedException {
		Yaml yaml = new Yaml();
        FileInputStream in = new FileInputStream("config.yaml");
		Map<String, Object> config = yaml.load(in);

        // 自动赋值到 Settings
        for (Field field : Settings.class.getFields()) {
            Object value = config.get(field.getName());
            if (value != null) {
                Class<?> type = field.getType();
				try {
					if (type == int.class) {
						field.setInt(null, Integer.parseInt(value.toString()));
					} else if (type == double.class) {
						field.setDouble(null, Double.parseDouble(value.toString()));
					} else if (type == boolean.class) {
						field.setBoolean(null, Boolean.parseBoolean(value.toString()));
					} else if (type == String.class) {
						field.set(null, value.toString());
					} else if (type == int[].class && value instanceof List) {
						List<?> list = (List<?>) value;
						int[] arr = list.stream().mapToInt(o -> Integer.parseInt(o.toString())).toArray();
						field.set(null, arr);
					}
                // 可扩展支持更多类型
				} catch (IllegalAccessException e) {
					throw new RuntimeException("Failed to set field: " + field.getName(), e);
				}
            }
        }
		
		
		DecimalFormat df = new DecimalFormat("000000.00");
		PrintWriter log = new PrintWriter(Settings.PATH_OUTPUT + "_log");
		log.println("Logfile");
		log.println("~~~~~~~\n");
		
		long indexStartTime = System.currentTimeMillis();
		
		TripleSet ts = new TripleSet(Settings.PATH_TRAINING, true);
	
		//DecimalFormat df = new DecimalFormat("0.0000");
		//System.out.println("MEMORY REQUIRED (before precomputeNRandomEntitiesPerRelatio): " + df.format(Runtime.getRuntime().totalMemory() / 1000000.0) + " MByte at " + System.currentTimeMillis());
		ts.setupListStructure();
		ts.precomputeNRandomEntitiesPerRelation(Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS);
		System.out.println(" done.");
		//System.out.println("MEMORY REQUIRED (after precomputeNRandomEntitiesPerRelatio): " + df.format(Runtime.getRuntime().totalMemory() / 1000000.0) + " MByte at " + System.currentTimeMillis());
		
		
		
		long indexEndTime = System.currentTimeMillis();
		log.println("indexing dataset: " + Settings.PATH_TRAINING);
		log.println("time elapsed: " + (indexEndTime - indexStartTime) + "ms");
		log.println();
		log.println(IOHelper.getParams());
		log.flush();
		
		
		long now = System.currentTimeMillis();
		
		// Thread[] scorer = new Thread[Learn.WORKER_THREADS];
		dice = new Dice(Settings.PATH_DICE);
		dice.computeRelevenatScores();
		dice.saveScores();
		 // new
		
		activeThread = new boolean[Settings.WORKER_THREADS];
		stats = new int[Settings.WORKER_THREADS][3];
		 
		Scorer[] scorer = new Scorer[Settings.WORKER_THREADS];
		for (int threadCounter = 0; threadCounter < Settings.WORKER_THREADS; threadCounter++) {
			
			Thread.sleep(50);
			System.out.println("* creating worker thread #" + threadCounter);
			Scorer s = new Scorer(ts, threadCounter);
			
			int type = dice.ask(0);
			boolean zero = Dice.decodedDiceZero(type);
			boolean cyclic = Dice.decodedDiceCyclic(type);
			boolean acyclic = Dice.decodedDiceAcyclic(type);
			int len = Dice.decodedDiceLength(type);
			s.setSearchParameters(zero, cyclic, acyclic, len);
			
			scorer[threadCounter] = s;
			scorer[threadCounter].start();
			activeThread[threadCounter] = true;
			
		}
		
		
		dice.resetScores();
		
		boolean done = false;
		
		int batchCounter = 0;
		
		
		// =================
		// === MAIN LOOP ===
		// =================
		
		long startTime = System.currentTimeMillis();
				
	
		int snapshotIndex = 0;
		long batchStart = System.currentTimeMillis();
		while (done == false) {
			// System.out.println("main thread sleeps for 10 ms");
			Thread.sleep(10);
			
			now  = System.currentTimeMillis();
			
			
			//elapsed seconds
			// snapshotIndex
			int elapsedSeconds = (int)(now - startTime) / 1000;
			int currentIndex = snapshotIndex;
			snapshotIndex = checkTimeMaybeStoreRules(log, done, snapshotIndex, elapsedSeconds, dice);
			if (snapshotIndex > currentIndex) {
				// this needs t be done to avoid that a zeror time batch conducted because of long rule storage times
				batchStart  = System.currentTimeMillis();
				now  = System.currentTimeMillis();
				// System.out.println("currentIndex=" +  currentIndex +  " snapshotIndex=" + snapshotIndex);
				// System.out.println("now: " + now);
			}
			
			
			if (now - batchStart > Settings.BATCH_TIME) {
				
				report = true;
				active = false;
				
				/// System.out.println(">>> set status to inactive");
				do {
					// System.out.println(">>> waiting for threads to report");
					Thread.sleep(10);
				} while(!allThreadsReported());
				batchCounter++;
				// saturation = getSaturationOfBatch();
				
				dice.computeRelevenatScores();
				dice.saveScores();
				
				
		
				int numOfRules = 0;
				for (int i = 0; i < 307; i++) numOfRules += rules307[i].size();
				
				// System.out.print(">>> Batch #" + batchCounter + " [" + numOfRules + " mined in " + (now - startTime) + "ms] ");
				// System.out.print(">>> Batch #" + batchCounter + " ");
				// System.out.println("MEMORY REQUIRED: " + df.format(Runtime.getRuntime().totalMemory() / 1000000.0) + " MByte");
				
				for (int t = 0; t < scorer.length; t++) {
					int type = dice.ask(batchCounter);
					// System.out.print(type + "|");
					boolean zero = Dice.decodedDiceZero(type);
					boolean cyclic = Dice.decodedDiceCyclic(type);
					boolean acyclic = Dice.decodedDiceAcyclic(type);
					int len = Dice.decodedDiceLength(type);
					scorer[t].setSearchParameters(zero, cyclic, acyclic, len);
				}
				// System.out.print("   ");
				
				System.out.println(dice);
				
				dice.resetScores();

				activateAllThreads(scorer);		
				batchStart  = System.currentTimeMillis();
				active = true;
				report = false;
			}
		}
		// =================
		
		
		
		log.flush();
		log.close();

		
	}

	private static int checkTimeMaybeStoreRules(PrintWriter log, boolean done, int snapshotIndex, int elapsedSeconds, Dice dice) {
		if (elapsedSeconds > Settings.SNAPSHOTS_AT[snapshotIndex] || done) {
			active = false;
			// this time might be required for letting the other threads go on one line in the code
			try { Thread.sleep(50); } catch (InterruptedException e1) { e1.printStackTrace(); }
			// ArrayList<Set<? extends Rule>> allUsefulRules = new ArrayList<Set<? extends Rule>>();


			// ...
			
			if (!done) System.out.println("\n>>> CREATING SNAPSHOT " + snapshotIndex + " after " + elapsedSeconds + " seconds");
			else System.out.println("\n>>> CREATING FINAL SNAPSHOT 0 after " + elapsedSeconds + " seconds");
			String suffix = "" + (done ? 0 : Settings.SNAPSHOTS_AT[snapshotIndex]);
			rwt = new RuleWriterAsThread(Settings.PATH_OUTPUT, done ? 0 : Settings.SNAPSHOTS_AT[snapshotIndex], rules307, log, elapsedSeconds);
			rwt.start();
			// storeRules(Settings.PATH_OUTPUT, done ? 0 : Settings.SNAPSHOTS_AT[snapshotIndex], rulesSyn, log, elapsedSeconds);
			System.out.println();
			dice.write(suffix);
			snapshotIndex++;
			if (snapshotIndex == Settings.SNAPSHOTS_AT.length || done) {
				log.close();
				System.out.println(">>> Bye, bye.");
				Learn.finished = true;
				
				
				while (rwt != null && rwt.isAlive()) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					System.out.println(">>> waiting for rule writer thread to finish");
				}
				
				
				System.exit(1);
			}
			active = true;
		}
		return snapshotIndex;
	}
	
	

	
	
	public static void printStatsOfBatch() {
		for (int i = 0; i < stats.length; i++) {
			System.out.print("Worker #" + i + ": ");
			for (int j = 0; j < stats[i].length - 1; j++) {
				System.out.print(stats[i][j] + " / ");
			}
			System.out.println(stats[i][stats[i].length-1]);
		}
	}
	
	public static double getSaturationOfBatch() {
		int storedTotal = 0;
		int createdTotal = 0;
		for (int i = 0; i < stats.length; i++) {
			storedTotal += stats[i][0];
			createdTotal += stats[i][1];
		}
		return 1.0 - ((double)storedTotal / (double)createdTotal);
	}
	
	
	/**
	* Called by a worker thread to see if batch time is over. If this is the case the thread is deactivated
	* and the thread specific statistics of its performance in the current batch are stored.
	* 
	* @param threadId Id of the thread reporting.
	* @param storedRules the number of good rule that have been stored.
	* @param createdRules The number of created rules that have been created and checked for novelty and quality.
	* @return
	*/
	public static boolean active(int threadId, int storedRules, int createdRules, double producedScore, boolean zero, boolean cyclic, boolean acyclic, int len) {
		if (active) return true;
		if (!report) return true;
		else if(activeThread[threadId]) {
			
			// System.out.println("retrieved message from thread " + threadId + " created=" + createdRules + " stored=" + storedRules + " produced=" +producedScore);
			int type = Dice.encode(zero, cyclic, acyclic, len);
			// System.out.println("type of thread: " + type + " cyclic=" + cyclic + " len=" + len);
			stats[threadId][0] = storedRules;
			stats[threadId][1] = createdRules;
			// connect to the dice 
			// TODO
			//int type = Dice.encode(cyclic, len);
			dice.addScore(type, producedScore);
			activeThread[threadId] = false;
			return false;
		}
		return false;

	}
	
	/**
	*  Checks whether all worker threads have reported and are deactivated.
	*  
	* @return True, if all threads have reported and are thus inactive.
	*/
	public static boolean allThreadsReported() {
		for (int i = 0; i < activeThread.length; i++) {
			if (activeThread[i] == true) return false;
		}
		return true;
	}
	
	/**
	* Activates all threads .
	* 
	* @param scorer The set of threads to be activated.
	*/
	public static void activateAllThreads(Thread[] scorer) {
		for (int i = 0; i < activeThread.length; i++) {
			activeThread[i] = true;
		}
		active = true;
	}
	


	public static void showElapsedMoreThan(long duration, String message) {
		long now = System.currentTimeMillis();
		long elapsed = now - timeStamp;
		if (elapsed > duration) {
			System.err.println(message + " required " + elapsed + " millis!");
		}
		
	}

	public static void takeTime() {
		timeStamp = System.currentTimeMillis();
		
	}



	
	/**
	 * Stores a given rule in a set. If the rule is a cyclic rule it also stores it in a way that is can be checked in
	 * constants time for a AC1 rule if the AC1 follows.
	 * 
	 * @param learnedRule
	 */
	public static void storeRule(Rule rule) {
		int code307 =  Math.abs(rule.hashCode()) % 307;
		rules307[code307].add(rule);
		// rulesSyn.add(rule);
		/*
		if (rule instanceof RuleCyclic) {
			indexXYRule((RuleCyclic)rule);
		}
		*/
	}
	
	/*
	private static synchronized void indexXYRule(RuleCyclic rule) {
		StringBuilder sb = new StringBuilder();
		sb.append(rule.getHead().toString());
		for (int i = 0; i < rule.bodysize(); i++) { sb.append(rule.getBodyAtom(i).toString()); }
		String rs = sb.toString();
		if (indexedXYRules.containsKey(rs)) {
			// should not happen
		}
		else {
			indexedXYRules.put(rs, rule);
		}
	}
	*/

	/**
	 * Checks if the given rule is already stored. 
	 *
	 */
	public static boolean isStored(Rule rule) {
		int code307 = Math.abs(rule.hashCode()) % 307;
		if (!rules307[code307].contains(rule)) {
			return true;
		}
		return false;
	}

	public static boolean areAllThere() {
		// System.out.println("there are " + availableThreads.size() + " threads here" );
		if (availableThreads.size() == Settings.WORKER_THREADS) return true;
		return false;
	}

	public static void heyYouImHere(int id) {
		availableThreads.add(id);
	}
	
	
	


}
