package org.virtuslab.scoozie
package conversion

case class RefWrap[T <: AnyRef](value: T) {
    override def equals(other: Any): Boolean = other match {
        case ref: RefWrap[_] => ref.value eq value
        case _               => false
    }
}

