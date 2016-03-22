package org.apache.kafka.streams.kstream.internals

import com.zendesk.maxwelljoin.{KStreamMapKey, KeyMapper}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{KeyValueMapper, ValueJoiner, KTable, KStreamBuilder}
import org.apache.kafka.streams.processor.ProcessorSupplier

class MaxwellKTable[K, S, V](topology: KStreamBuilder,
                                  name: String,
                                  ps: ProcessorSupplier[_, _],
                                  sourceNodes: java.util.Set[String])
  extends KTableImpl[K, S, V](topology, name, ps, sourceNodes) {


  def this(kt: KTableImpl[K, S, V]) = {
    this(kt.topology, kt.name, kt.processorSupplier, kt.sourceNodes)
  }

  def join[K1, V1, V2, R](other: KTable[K, V1],
                          leftMapper: KeyMapper[K, K1, V],
                          rightMapper: KeyMapper[K, K1, V],
                          joiner: ValueJoiner[V, V1, R]): KTable[K, R] = {
    val allSourceNodes: java.util.Set[String] = ensureJoinableWith(other.asInstanceOf[AbstractStream[K]])

    val joinLMapperName = topology.newName("maxwell-join-left-mapper")
    val joinRMapperName = topology.newName("maxwell-join-right-mapper")
    val joinThisName    = topology.newName("maxwell-join")
    val joinOtherName   = topology.newName("maxwell-outer-join")
    val joinMergeName   = topology.newName("maxwell-merge-join")

    val joinLMapper: KStreamMapKey[K, K1, V] = new KStreamMapKey(leftMapper)
    val joinThis: KTableKTableJoin[K, R, V, V1] = new KTableKTableJoin(this, other.asInstanceOf[KTableImpl[K, _, V1]], joiner)
    val joinOther: KTableKTableJoin[K, R, V1, V] = new KTableKTableJoin(other.asInstanceOf[KTableImpl[K, _, V1]], this, reverseJoiner(joiner))
    val joinMerge: KTableKTableJoinMerger[K, R] = new KTableKTableJoinMerger(new KTableImpl[K, V, R](topology, joinThisName, joinThis, this.sourceNodes), new KTableImpl[K, V1, R](topology, joinOtherName, joinOther, (other.asInstanceOf[KTableImpl[K, _, _]]).sourceNodes))

    topology.addProcessor(joinLMapperName, joinLMapper, this.name)
    topology.addProcessor(joinThisName, joinThis, joinLMapperName)
    topology.addProcessor(joinOtherName, joinOther, (other.asInstanceOf[KTableImpl[_, _, _]]).name)
    topology.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName)

    return new KTableImpl[K, AnyRef, R](topology, joinMergeName, joinMerge, allSourceNodes)
  }

  def reverseJoiner[T2, T1, R](joiner: ValueJoiner[T1, T2, R]): ValueJoiner[T2, T1, R] = {
    return new ValueJoiner[T2, T1, R]() {
      def apply(value2: T2, value1: T1): R = {
        return joiner.apply(value1, value2)
      }
    }
  }
}

object MaxwellKTable {
  def apply[K, V](kt: KTable[K, V]) = new MaxwellKTable(kt.asInstanceOf[KTableImpl[K, V, V]])
}


