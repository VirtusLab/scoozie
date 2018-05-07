package org.virtuslab.scoozie
package conversion

case class RefSet[A <: AnyRef](vals: Seq[A]) extends Set[A] {

    override def contains(elem: A): Boolean = vals exists (e => e eq elem)

    def iterator: Iterator[A] = vals.iterator

    def -(elem: A): RefSet[A] = {
        if (!(this contains elem))
            this
        else
            RefSet(vals filter (_ ne elem))
    }

    def +(elem: A): RefSet[A] = {
        if (this contains elem)
            this
        else
            RefSet(vals :+ elem)
    }
    def ++(elems: RefSet[A]): RefSet[A] = (this /: elems)(_ + _)

    def --(elems: RefSet[A]): RefSet[A] = (this /: elems)(_ - _)

    def map[B <: AnyRef](f: (A) => B): RefSet[B] = {
        (RefSet[B]() /: vals) ((e1: RefSet[B], e2: A) => e1 + f(e2))
    }

    override def equals(that: Any): Boolean = {
        that match {
            case RefSet(otherVals) =>
                this.vals.toSet.equals(otherVals.toSet)
            case _ => false
        }
    }

}

object RefSet {
    def apply[A <: AnyRef](): RefSet[A] = {
        RefSet(Seq.empty)
    }
    def apply[A <: AnyRef](elem: A, elems: A*): RefSet[A] = {
        RefSet(elem +: elems)
    }
}
