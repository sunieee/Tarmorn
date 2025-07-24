package tarmorn.eval

import tarmorn.data.TripleSet

object TestHardnessEstimator {
    @JvmStatic
    fun main(args: Array<String>) {
        val training = TripleSet("../AnyBURL/data/FB237/train.txt")
        val test = TripleSet("../AnyBURL/data/FB237/test.txt")
        val valid = TripleSet("../AnyBURL/data/FB237/valid.txt")


        /*
		TripleSet training = new TripleSet("data/FB15k/train.txt");
		TripleSet test = new TripleSet("data/FB15k/test.txt");
		TripleSet valid = new TripleSet("data/FB15k/valid.txt");
		*/
        val all = TripleSet()
        all.addTripleSet(training)
        all.addTripleSet(test)
        all.addTripleSet(valid)


        val headsCounter = IntArray(5)
        val tailsCounter = IntArray(5)


        val unknownRateHead = 0.1
        val unknownRateTail = 0.1

        var hitsAT1RateHead = 0.0
        var hitsAT1RateTail = 0.0

        for (t in test.triples) {
            //System.out.print(t);

            val head = t.h
            val tail = t.t
            val relation = t.r

            val heads = all.getHeadEntities(relation, tail)
            val tails = all.getTailEntities(relation, head)

            var d1 = 1.0 / (heads!!.size * unknownRateHead)
            if (d1 >= 1.0) d1 = 1.0
            hitsAT1RateHead += d1

            var d2 = 1.0 / (tails!!.size * unknownRateTail)
            if (d2 >= 1.0) d2 = 1.0
            hitsAT1RateTail += d2


            // println(d);


            // println(tails.size() + "|" + heads.size() + " |" +  t);
            if (heads.size == 1) headsCounter[0]++
            if (heads.size > 1 && heads.size <= 10) headsCounter[1]++
            if (heads.size > 10 && heads.size <= 50) headsCounter[2]++
            if (heads.size > 50 && heads.size <= 200) headsCounter[3]++
            if (heads.size > 200) headsCounter[4]++

            if (tails.size == 1) tailsCounter[0]++
            if (tails.size > 1 && tails.size <= 10) tailsCounter[1]++
            if (tails.size > 10 && tails.size <= 50) tailsCounter[2]++
            if (tails.size > 50 && tails.size <= 200) tailsCounter[3]++
            if (tails.size > 200) tailsCounter[4]++
        }

        var allHeads = 0
        var allTails = 0
        for (i in 0..4) {
            allHeads += headsCounter[i]
            allTails += tailsCounter[i]
        }

        for (i in 0..4) {
            print((headsCounter[i] / allHeads.toDouble()).toString() + "\t")
        }
        println()

        for (i in 0..4) {
            print((tailsCounter[i] / allTails.toDouble()).toString() + "\t")
        }
        println()


        println(headsCounter[0].toString() + "\t" + headsCounter[1] + "\t" + headsCounter[2] + "\t" + headsCounter[3] + "\t" + (headsCounter[4].toDouble() / allTails))
        println(tailsCounter[0].toString() + "\t" + tailsCounter[1] + "\t" + tailsCounter[2] + "\t" + tailsCounter[3] + "\t" + tailsCounter[4])

        println("hitsAT1RateHead =     " + hitsAT1RateHead / test.triples.size)
        println("hitsAT1RateTail =     " + hitsAT1RateTail / test.triples.size)
        println("average hitsAT1Rate = " + ((hitsAT1RateTail / test.triples.size) + (hitsAT1RateHead / test.triples.size)) / 2.0)

        // println("hitsAT1RateTail = " + hitsAT1RateTail / test.getTriples().size());
    }
}
