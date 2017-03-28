package edu.nyu.tandon.search.selective

/**
  * @author michal.siedlaczek@nyu.edu
  */
object Path {

  val CostSuffix = ".cost"
  val PostingCostSuffix = ".postingcost"
  val PayoffSuffix = ".payoff"
  val QueriesSuffix = ".queries"
  val QueryLengthsSuffix = ".lengths"
  val SelectionSuffix = ".selection"
  val ShardCountSuffix = ".shard-count"
  val SelectedSuffix = ".selected"
  val ResultsSuffix = ".results"
  val DocumentsSuffix = ".docs"
  val LocalSuffix = ".local"
  val GlobalSuffix = ".global"
  val ScoresSuffix = ".scores"
  val TrecSuffix = ".trec"
  val TrecIdSuffix = ".trecid"
  val TitlesSuffix = ".titles"
  val ReDDESuffix = ".redde"
  val ShRkCSuffix = ".shrkc"
  val LabelSuffix = ".label"
  val PayoffModelSuffix = ".payoff-model"
  val CostModelSuffix = ".cost-model"
  val EvalSuffix = ".eval"
  val OverlapsSuffix = ".overlaps"
  val OverlapSuffix = ".overlap"
  val PrecisionsSuffix = ".precisions"
  val PrecisionSuffix = ".precision"
  val QRelsSuffix = ".qrels"

  private def toAny(basename: String, suffix: String): String = s"$basename$suffix"
  private def toAny(basename: String, shardId: Int, suffix: String): String = s"$basename#$shardId$suffix"
  private def toAny(basename: String, shardId: Int, bucketId: Int, suffix: String): String = s"$basename#$shardId#$bucketId$suffix"

  /* Query level */
  def toQueries(basename: String): String = toAny(basename, QueriesSuffix)
  def toSelection(basename: String): String = toAny(basename, SelectionSuffix)
  def toSelectionShardCount(basename: String): String = toAny(basename, s"$SelectionSuffix$ShardCountSuffix")
  def toSelectedDocuments(basename: String): String = toAny(basename, s"$SelectedSuffix$DocumentsSuffix")
  def toSelectedScores(basename: String): String = toAny(basename, s"$SelectedSuffix$ScoresSuffix")
  def toSelectedTrec(basename: String): String = toAny(basename, s"$SelectedSuffix$TrecSuffix")
  def toTrec(basename: String): String = toAny(basename, TrecSuffix)
  def toTrecIds(basename: String): String = toAny(basename, TrecIdSuffix)
  def toPayoffModel(basename: String): String = toAny(basename, PayoffModelSuffix)
  def toPayoffModelEval(basename: String): String = s"${toPayoffModel(basename)}$EvalSuffix"
  def toCostModel(basename: String): String = toAny(basename, CostModelSuffix)
  def toCostModelEval(basename: String): String = s"${toCostModel(basename)}$EvalSuffix"
  def toOverlaps(basename: String, k: Int): String = toAny(basename, s"@$k$OverlapsSuffix")
  def toOverlap(basename: String, k: Int): String = toAny(basename, s"@$k$OverlapSuffix")
  def toPrecisions(basename: String, k: Int): String = toAny(basename, s"@$k$PrecisionsSuffix")
  def toPrecision(basename: String, k: Int): String = toAny(basename, s"@$k$PrecisionSuffix")
  def toQRels(basename: String): String = toAny(basename, QRelsSuffix)

  /* Shard level */
  def toCosts(basename: String, shardId: Int): String = toAny(basename, shardId, CostSuffix)

  /* Bucket level */
  def toCosts(basename: String, shardId: Int, bucketId: Int): String = toAny(basename, shardId, bucketId, CostSuffix)
  def toPostingCosts(basename: String, shardId: Int, bucketId: Int): String = toAny(basename, shardId, bucketId, PostingCostSuffix)
  def toPayoffs(basename: String, shardId: Int, bucketId: Int): String = toAny(basename, shardId, bucketId, PayoffSuffix)
  def toLocalResults(basename: String): String = toAny(basename, s"$ResultsSuffix$LocalSuffix")
  def toGlobalResults(basename: String): String = toAny(basename, s"$ResultsSuffix$GlobalSuffix")
  def toScores(basename: String): String = toAny(basename, s"$ResultsSuffix$ScoresSuffix")
  def toScores(basename: String, shardId: Int, bucketId: Int): String = toAny(basename, shardId, bucketId, s"$ResultsSuffix$ScoresSuffix")
  def toGlobalResults(basename: String, shardId: Int, bucketId: Int): String = toAny(basename, shardId, bucketId, s"$ResultsSuffix$GlobalSuffix")
  def toDocRank(basename: String, shardId: Int, bucketId: Int): String = toAny(basename, shardId, bucketId, ".docrank")
  def toBucketRank(basename: String, shardId: Int, bucketId: Int): String = toAny(basename, shardId, bucketId, ".bucketrank")

//  def absolute(path: String): String = path.head match {
//    case '/' => path
//    case _   => s"${new File(path).getAbsolutePath}"
//  }

}
