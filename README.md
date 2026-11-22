# Daylin Stream Queue
The goal of this project is to build a system that allows event producers to connect to this service, send an event,
then establish a webstream link that the two can stream data over.  This allows for systems where a producer needs to
stream data and talk to some kind of interactive services consuming the event.  This is nice for things like a build
service for remotely building projects where you need to stream logs, results, and keyboard events to and from the user.

## Architecture
This architecture of this project is similar to that of a more complex version of RabbitMQ.  For this, messages are
described via a config.yml file that determines what the types of events that can be sent, how and where they are sent,
and what mode the event is handled with.