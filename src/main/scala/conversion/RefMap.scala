package com.klout.scoozie
package conversion

/*
 * Map that compares keys by reference
 */
case class RefMap[A <: AnyRef, B](vals: Map[RefWrap[A], B]) extends Map[RefWrap[A], B] {

    def +[B1 >: B](kv: (RefWrap[A], B1)): RefMap[A, B1] = {
        RefMap(vals + kv)
    }

    def +[B1 >: B](kv: => (A, B1)): RefMap[A, B1] = {
        val newKv = RefWrap(kv._1) -> kv._2
        RefMap(vals + newKv)
    }

    def ++(rmap: RefMap[A, B]): RefMap[A, B] = {
        (this /: rmap) (_ + _)
    }

    def -(key: RefWrap[A]): RefMap[A, B] = {
        RefMap(vals - key)
    }

    def get(key: RefWrap[A]): Option[B] = {
        vals get key
    }

    def get(key: => A): Option[B] = {
        vals get RefWrap(key)
    }

    def iterator: Iterator[(RefWrap[A], B)] = vals.iterator
}
