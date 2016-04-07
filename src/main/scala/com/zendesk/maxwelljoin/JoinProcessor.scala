package com.zendesk.maxwelljoin
class JoinProcessor(joinDefs: List[JoinDef]) extends AbstractJoinProcessor(joinDefs) {

  def performJoins(alias: String, database: String, data: MaxwellData): MaxwellData = {
    val tableJoins = joinDefs.filter(_.thisAlias == alias)

    tableJoins.foldLeft(data) { (d, join) =>
      d.get(join.thisField).map { joinValue =>
        val isJoinToPK = tableInfo.isKeyPrimary(database, join.thatTable, join.thatField)
        val lookupKey = MaxwellKey(database, join.thatTable, List(join.thatField -> joinValue))

        if (isJoinToPK) {
          val maybeRightData = indexStore.getData(lookupKey)

          maybeRightData.map { rightData =>
            val joinedRightData = performJoins(join.thatAlias, database, rightData)
            d + (join.thatAlias -> joinedRightData)
          }.getOrElse(d)
        } else {
          val rightData =
            for (
              key <- indexStore.getIndex(lookupKey);
              rightData <- indexStore.getData(key)
            ) yield rightData

          val joinedRightData = rightData.map { r => performJoins(join.thatAlias, database, r) }
          d + (join.thatAlias -> joinedRightData)
        }
      }.getOrElse(d)
        //.getOrElse(data)
    }
    // start with ticket in hand

    // get all right-hand joins to this table

  }

  override def process(key: MaxwellKey, value: MaxwellValue): Unit = {
    val data = performJoins(key.table, key.database, value.data)
    context.forward(key, data)
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
