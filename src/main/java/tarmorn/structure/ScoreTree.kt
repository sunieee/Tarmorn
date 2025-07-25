package tarmorn.structure

import tarmorn.Settings
import kotlin.math.pow
import java.util.LinkedList
import java.util.LinkedHashMap

class ScoreTree {
    var children: MutableList<ScoreTree>
    private var score = 0.0
    private val explanation: Rule?
    private var storedValues: MutableSet<String>?
    private var numOfValues: Int
    private var index = 0
    private val root: Boolean
    private var closed = false


    //private numOf
    constructor() {
        this.children = mutableListOf<ScoreTree>()
        this.storedValues = null
        this.closed = false
        this.numOfValues = 0
        this.index = 0
        this.root = true
        this.explanation = null
    }

    constructor(score: Double, values: MutableSet<String>, explanation: Rule?) {
        this.score = score
        this.explanation = explanation
        this.children = mutableListOf<ScoreTree>()
        this.storedValues = mutableSetOf<String>()
        this.storedValues!!.addAll(values)
        if (this.storedValues!!.size <= 1) this.closed = true
        else this.closed = false
        this.numOfValues = values.size
        this.root = false
    }


    fun addValues(score: Double, values: MutableSet<String>, explanation: Rule?) {
        //for(String v : values) {
        //	println(score + ": " + v);
        //}
        this.addValues(score, values, explanation, 0)
    }

    private fun addValues(score: Double, values: MutableSet<String>, explanation: Rule?, counter: Int) {
        // go deep first
        for (child in this.children) {
            // int nosv = this.storedValues != null ? this.storedValues.size() : 0;
            child.addValues(score, values, explanation, 0)
        }
        // compare with stored values
        val touched = HashSet<String>()
        val untouched = HashSet<String>()
        if (!root) {
            for (storedValue in this.storedValues!!) {
                if (values.contains(storedValue)) touched.add(storedValue)
                else untouched.add(storedValue)
            }
            values.removeAll(touched)
        }
        // standard split
        if (touched.size > 0 && this.storedValues!!.size > 1 && touched.size < this.storedValues!!.size) {
            val childIndex = this.index - untouched.size
            if (childIndex >= LOWER_BOUND) {
                this.storedValues = touched
                this.index = childIndex
                this.numOfValues -= untouched.size
            } else {
                this.storedValues = untouched
                this.addChild(score, touched, explanation, childIndex)
            }
        }
        // special case of adding new value, which happens only if the maximal number of values is not yet exceeded
        if (this.root && values.size > 0 && this.numOfValues < LOWER_BOUND) {
            this.addChild(score, values, explanation, this.numOfValues + values.size)
            this.numOfValues += values.size
        }


        // try to set on closed if only 1 or less values are stored in this and in its children
        if (this.storedValues == null || this.storedValues!!.size <= 1) {
            var c = true
            for (child in this.children) {
                if (!child.closed) {
                    c = false
                    break
                }
            }
            this.closed = c
        }
    }

    private fun addChild(score: Double, values: MutableSet<String>, explanation: Rule?, childIndex: Int): ScoreTree {
        val child = ScoreTree(score, values, explanation)
        child.index = childIndex
        this.children.add(child)
        return child
    }


    override fun toString(): String {
        var rep = ""
        for (child in children) {
            rep = rep + child.toString("")
        }
        return rep
    }

    private fun toString(indent: String): String {
        var rep = ""
        val closingSign = if (this.closed) "X" else "O"
        rep += indent + closingSign + " " + this.score + " [" + this.index + "](" + this.numOfValues + ") -> { "
        if (storedValues != null) {
            for (v in this.storedValues) {
                rep += v + " "
            }
        }
        rep += "} with explanation: " + this.explanation + "\n"
        for (child in children) {
            rep = rep + child.toString(indent + "   ")
        }
        return rep
    }

    fun print(ss: String, set: HashSet<String>) {
        print(ss + ": ")
        for (s in set) {
            print(s + ",")
        }
        println()
    }


    fun fine(): Boolean {
        if (this.root && this.children.size > 0) {
            val i = this.children.get(children.size - 1).index
            // println(i);
            if (i >= LOWER_BOUND && i <= UPPER_BOUND) {
                return this.isFirstUnique
            }
        }
        return false
    }

    private val isFirstUnique: Boolean
        get() {
            var tree = this
            while (tree.children.size > 0) {
                tree = tree.children.get(0)
            }
            return tree.closed
        }

    fun getAsLinkedList(list: LinkedHashMap<String, Double>, ps: Double, level: Int, myself: String) {
        if (this.children.size > 0) {
            for (child in children) {
                if (this.root) {
                    child.getAsLinkedList(list, ps, level + 1, myself)
                } else {
                    val psUpdated = ps + EPSILON.pow((level - 1).toDouble()) * this.score
                    child.getAsLinkedList(list, psUpdated, level + 1, myself)
                }
            }
        }
        if (!this.root) {
            val psUpdated = ps + EPSILON.pow((level - 1).toDouble()) * this.score
            // print("" + psUpdated, this.storedValues);
            for (v in this.storedValues!!) {
                var value: String = v
                list.put(value, psUpdated)
            }
        }
    }

    fun getAsLinkedList(list: LinkedHashMap<String, Double>, myself: String) {
        this.getAsLinkedList(list, 0.0, 0, myself)
    }


    val explainedCandidates: HashMap<String, HashSet<Rule>>
        get() {
            val explainedCandidates =
                HashMap<String, HashSet<Rule>>()
            val explanations = LinkedList<Rule>()
            this.getExplainedCandidates(explainedCandidates, explanations, 0)
            return explainedCandidates
        }

    fun getExplainedCandidates(
        explainedCandidates: HashMap<String, HashSet<Rule>>,
        explanations: LinkedList<Rule>,
        level: Int
    ) {
        if (this.children.size > 0) {
            for (child in children) {
                if (this.root) {
                    child.getExplainedCandidates(explainedCandidates, explanations, level + 1)
                } else {
                    // double psUpdated = ps + Math.pow(EPSILON, level-1) * this.score;
                    explanations.add(this.explanation!!)
                    child.getExplainedCandidates(explainedCandidates, explanations, level + 1)
                    explanations.removeLast()
                }
            }
        }
        if (!this.root) {
            val collectedExplanations = HashSet<Rule>()
            collectedExplanations.addAll(explanations)
            for (v in this.storedValues!!) {
                explainedCandidates.put(v, collectedExplanations)
            }
        }
    }


    companion object {
        @JvmField
        var LOWER_BOUND: Int = 10
        @JvmField
        var UPPER_BOUND: Int = 10

        @JvmField
        var EPSILON: Double = 0.00001


        @JvmStatic
        fun main(args: Array<String>) {
            val tree = ScoreTree()
            val s1 = HashSet<String>()
            s1.add("a")
            s1.add("b")
            s1.add("c")
            s1.add("d")
            s1.add("d1")
            s1.add("d2")
            tree.addValues(0.9, s1, null)

            println("-------")
            println(tree)
            println("precise enough " + tree.fine())
            println("first unique   " + tree.isFirstUnique)

            val s11 = HashSet<String>()
            s11.add("aaa")
            s11.add("bbb")
            tree.addValues(0.8999, s11, null)

            println("-------")
            println(tree)
            println("precise enough " + tree.fine())
            println("first unique   " + tree.isFirstUnique)

            val s12 = HashSet<String>()
            s12.add("a")
            s12.add("b")
            tree.addValues(0.891, s12, null)

            println("-------")
            println(tree)
            println("precise enough " + tree.fine())
            println("first unique   " + tree.isFirstUnique)

            val s2 = HashSet<String>()
            s2.add("a")
            s2.add("b")
            s2.add("e1")
            s2.add("e2")
            s2.add("e3")
            s2.add("e4")
            tree.addValues(0.88, s2, null)


            println("-------")
            println(tree)
            println("precise enough " + tree.fine())
            println("first unique   " + tree.isFirstUnique)

            val s3 = HashSet<String>()
            s3.add("a")
            s3.add("e1")
            s3.add("e4")
            // s3.add("e5");
            tree.addValues(0.6, s3, null)


            println("-------")
            println(tree)
            println("precise enough " + tree.fine())
            println("first unique   " + tree.isFirstUnique)

            val s4 = HashSet<String>()
            s4.add("xa")
            s4.add("xb")
            s4.add("xe1")
            s4.add("xe2")
            s4.add("xe3")
            s4.add("xe4")
            tree.addValues(0.5, s4, null)


            println("-------")
            println(tree)
            println("precise enough " + tree.fine())

            val s5 = HashSet<String>()
            s5.add("xa")
            s5.add("xb")
            s5.add("ye1")
            s5.add("ye2")
            s5.add("ye3")
            s5.add("ye4")
            tree.addValues(0.41, s5, null)


            println("-------")
            println(tree)
            println("precise enough " + tree.fine())

            val s6 = HashSet<String>()
            s6.add("xe2")
            s6.add("xa")
            s6.add("xxx")
            //s6.add("xb");
            tree.addValues(0.39, s6, null)

            println("-------")
            println(tree)
            println("precise enough " + tree.fine())

            val s7 = HashSet<String>()
            s7.add("xe1")
            s7.add("xe2")
            tree.addValues(0.22, s7, null)


            println("-------")
            println(tree)
            println("precise enough " + tree.fine())

            val s8 = HashSet<String>()
            // s8.add("xe1");
            s8.add("xe4")
            tree.addValues(0.21, s8, null)


            println("-------")
            println(tree)
            println("precise enough " + tree.fine())

            val s9 = HashSet<String>()
            // s8.add("xe1");
            s9.add("e1")
            s9.add("e2")
            tree.addValues(0.21, s9, null)

            println("-------")
            println(tree)
            println("precise enough " + tree.fine())

            val s19 = HashSet<String>()
            // s8.add("xe1");
            s19.add("u1")
            s19.add("u2")
            tree.addValues(0.19, s19, null)

            println("-------")
            println(tree)
            println("precise enough " + tree.fine())

            val list = LinkedHashMap<String, Double>()
            tree.getAsLinkedList(list, 0.0, 0, "blubber")

            for (e in list.entries) {
                println(e.value.toString() + ": " + e.key)
            }
        }
    }
}
