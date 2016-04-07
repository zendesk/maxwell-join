package com.zendesk.maxwelljoin

class JoinProcessor(joinDefs: List[JoinDef]) extends AbstractJoinProcessor(joinDefs) {

  def performJoin(key: MaxwellKey, data: MaxwellData, join: JoinDef) = {
    // start with ticket in hand

    // get all right-hand joins to this table
    val joins = joinDefs.filter(_.thisTable == key.table)

  }

  override def process(key: MaxwellKey, value: MaxwellValue): Unit = {
    context.forward(key, value.data)
  }

  def processRightPointingJoin(key: MaxwellKey, data: MaxwellData, join: JoinDef): MaxwellData = {
    data.get(join.thisField).map { refValue =>
      val isJoinToPK = tableInfo.isKeyPrimary(key.database, join.thatTable, join.thatField)
      val joinData = indexStore.getJoinData(key, refValue, join, isJoinToPK)

      if (isJoinToPK) {
        // has-one
        data + (join.thatAlias -> joinData.headOption.map(_._2))
      } else {
        // has-many
        data + (join.thatAlias -> joinData.map(_._2))
      }
    }.getOrElse(data)
  }

  /*
    if (rightHandJoins.size > 0) {
      context.forward(key, newData)
    }
*/
}
