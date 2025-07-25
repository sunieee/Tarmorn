package tarmorn.structure.compare

import tarmorn.structure.Body

class BodyLexicalComparator : Comparator<Body> {
    override fun compare(b1: Body, b2: Body): Int {
        return b1.toString().compareTo(b2.toString())
    }
}
