package edu.nyu.tandon.search.selective

/**
  * @author michal.siedlaczek@nyu.edu
  */
object Path {

  val CostSuffix = ".cost"
//  val DivisionSuffix = ".division"
  val PayoffSuffix = ".payoff"
  val QueriesSuffix = ".queries"
  val QueryLengthsSuffix = ".qlen"
  val SelectionSuffix = ".selection"
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
  val ModelSuffix = ".model"
  val EvalSuffix = ".eval"
  val OverlapsSuffix = ".overlaps"
  val OverlapSuffix = ".overlap"

  private def toAny(basename: String, suffix: String): String = s"$basename$suffix"
  private def toAny(basename: String, shardId: Int, suffix: String): String = s"$basename#$shardId$suffix"
  private def toAny(basename: String, shardId: Int, bucketId: Int, suffix: String): String = s"$basename#$shardId#$bucketId$suffix"

  /* Query level */
  def toQueries(basename: String): String = toAny(basename, QueriesSuffix)
  def toSelection(basename: String): String = toAny(basename, SelectionSuffix)
  def toSelectedDocuments(basename: String): String = toAny(basename, s"$SelectedSuffix$DocumentsSuffix")
  def toSelectedScores(basename: String): String = toAny(basename, s"$SelectedSuffix$ScoresSuffix")
  def toSelectedTrec(basename: String): String = toAny(basename, s"$SelectedSuffix$TrecSuffix")
  def toTrec(basename: String): String = toAny(basename, TrecSuffix)
  def toTrecIds(basename: String): String = toAny(basename, TrecIdSuffix)
  def toModel(basename: String): String = toAny(basename, ModelSuffix)
  def toModelEval(basename: String): String = s"${toModel(basename)}$EvalSuffix"
  def toOverlaps(basename: String, k: Int): String = toAny(basename, s"@$k$OverlapsSuffix")
  def toOverlap(basename: String, k: Int): String = toAny(basename, s"@$k$OverlapSuffix")

  /* Shard level */

  /* Bucket level */
  def toCosts(basename: String, shardId: Int, bucketId: Int): String = toAny(basename, shardId, bucketId, CostSuffix)
  def toPayoffs(basename: String, shardId: Int, bucketId: Int): String = toAny(basename, shardId, bucketId, PayoffSuffix)
  def toLocalResults(basename: String): String = toAny(basename, s"$ResultsSuffix$LocalSuffix")
  def toGlobalResults(basename: String): String = toAny(basename, s"$ResultsSuffix$GlobalSuffix")
  def toScores(basename: String): String = toAny(basename, s"$ResultsSuffix$ScoresSuffix")
  def toGlobalResults(basename: String, shardId: Int, bucketId: Int): String = toAny(basename, shardId, bucketId, s"$ResultsSuffix$GlobalSuffix")

}
