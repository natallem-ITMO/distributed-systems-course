package dijkstra.messages

sealed class Message

data class UpdateDistMessage(val dist: Long) : Message()

object AddChildMessage : Message()
object DeleteChildMessage : Message()
object AckMessage : Message()
