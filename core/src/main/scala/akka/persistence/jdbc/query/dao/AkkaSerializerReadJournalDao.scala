package akka.persistence.jdbc.query.dao
import akka.NotUsed
import akka.persistence.PersistentRepr
import akka.persistence.jdbc.config.ReadJournalConfig
import akka.persistence.jdbc.journal.dao.{ AkkaSerialization, BaseJournalDaoWithReadMessages }
import akka.serialization.Serialization
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.JdbcProfile

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

class AkkaSerializerReadJournalDao(
    val db: Database,
    val profile: JdbcProfile,
    val readJournalConfig: ReadJournalConfig,
    serialization: Serialization)(implicit val ec: ExecutionContext, val mat: Materializer)
    extends ReadJournalDao
    with BaseJournalDaoWithReadMessages {
  import profile.api._

  val queries = new ReadJournalQueries(profile, readJournalConfig)

  override def allPersistenceIdsSource(max: Long): Source[String, NotUsed] =
    Source.fromPublisher(db.stream(queries.allPersistenceIdsDistinct(max).result))

  override def eventsByTag(
      tag: String,
      offset: Long,
      maxOffset: Long,
      max: Long): Source[Try[(PersistentRepr, Set[String], Long)], NotUsed] = {

    // This doesn't populate the tags. AFAICT they aren't used
    Source
      .fromPublisher(db.stream(queries.eventsByTag(tag, offset, maxOffset, max).result))
      .map(row =>
        AkkaSerialization.fromRow(serialization)(row).map { case (repr, ordering) => (repr, Set.empty, ordering) })
  }

  override def journalSequence(offset: Long, limit: Long): Source[Long, NotUsed] =
    Source.fromPublisher(db.stream(queries.journalSequenceQuery(offset, limit).result))

  override def maxJournalSequence(): Future[Long] =
    db.run(queries.maxJournalSequenceQuery.result)

  override def messages(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      max: Long): Source[Try[(PersistentRepr, Long)], NotUsed] =
    Source
      .fromPublisher(db.stream(queries.messagesQuery(persistenceId, fromSequenceNr, toSequenceNr, max).result))
      .map(AkkaSerialization.fromRow(serialization))

}
