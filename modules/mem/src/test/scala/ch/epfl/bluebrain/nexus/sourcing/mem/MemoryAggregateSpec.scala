package ch.epfl.bluebrain.nexus.sourcing.mem

import cats.instances.try_._
import ch.epfl.bluebrain.nexus.sourcing.FixturesAggregate.Command.{AppendPermissions, DeletePermissions}
import ch.epfl.bluebrain.nexus.sourcing.FixturesAggregate.Event.{PermissionsAppended, PermissionsWritten}
import ch.epfl.bluebrain.nexus.sourcing.FixturesAggregate.State.Current
import ch.epfl.bluebrain.nexus.sourcing.FixturesAggregate._
import ch.epfl.bluebrain.nexus.sourcing.PersistentId
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import org.scalatest.{Matchers, TryValues, WordSpecLike}

import scala.util.Try

class MemoryAggregateSpec extends WordSpecLike with Matchers with TryValues {

  private val aggregate = MemoryAggregate("permission")(initial, next, eval).toF[Try]

  private def genPersistentId = PersistentId(genId)

  "A MemoryAggregateSpec" should {
    "return a 0 sequence number for an empty event log" in {
      aggregate.lastSequenceNr(PersistentId("unknown")).success.value shouldEqual 0L
    }
    "return a initial state for an empty event log" in {
      val id = genPersistentId
      aggregate.currentState(id).success.value shouldEqual initial
    }
    "append an event to the log" in {
      val id = genPersistentId
      aggregate.currentState(id).success.value shouldEqual initial
      aggregate.append(id, PermissionsWritten(own)).success.value shouldEqual 1L
      aggregate.currentState(id).success.value shouldEqual Current(own)

    }
    "retrieve the appended events from the log" in {
      val id = genPersistentId
      aggregate.append(id, PermissionsAppended(own))
      aggregate.append(id, PermissionsAppended(read))
      aggregate
        .foldLeft(id, List.empty[Event]) {
          case (acc, ev) => ev :: acc
        }
        .success
        .value shouldEqual List(PermissionsAppended(read), PermissionsAppended(own))
    }
    "reject out of order commands" in {
      val id = genPersistentId
      aggregate.eval(id, DeletePermissions).success.value match {
        case Left(_: Rejection) => ()
        case Right(_)           => fail("should have rejected deletion on initial state")
      }
    }
    "check of out of order commands" in {
      val id = genPersistentId
      aggregate.checkEval(id, DeletePermissions).success.value match {
        case Some(_: Rejection) => ()
        case None               => fail("should have rejected deletion on initial state")
      }
    }
    "return the current computed state" in {
      val id = genPersistentId
      val returned = for {
        _      <- aggregate.eval(id, AppendPermissions(own))
        second <- aggregate.eval(id, AppendPermissions(read))
      } yield second
      returned.success.value shouldEqual Right(Current(ownRead))
      aggregate.currentState(id).success.value shouldEqual Current(ownRead)
    }
    "return no rejection when check on commands evaluation is successful" in {
      val id = genPersistentId
      aggregate.checkEval(id, AppendPermissions(own)).success.value shouldEqual None
      aggregate.checkEval(id, AppendPermissions(read)).success.value shouldEqual None
      aggregate.currentState(id).success.value shouldEqual initial
    }
  }

}
