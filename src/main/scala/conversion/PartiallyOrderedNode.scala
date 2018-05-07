package com.virtuslab.scoozie
package conversion

case class PartiallyOrderedNode(
    node:         GraphNode,
    partialOrder: Int)

case object PartiallyOrderedNode {
    def lt(x: PartiallyOrderedNode, y: PartiallyOrderedNode): Boolean = {
        x.partialOrder < y.partialOrder || (x.partialOrder == y.partialOrder && x.node.name < y.node.name)
    }
}
