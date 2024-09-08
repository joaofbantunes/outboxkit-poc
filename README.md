# OutboxKit - Proof of Concept

This is a proof of concept of creating a toolkit to implement the outbox pattern.

The goal is not to provide a full implementation, but to create foundations to implement.

I'm going for something that is:

- somewhat generic, in order to accommodate different technologies, such as message brokers, databases and database access libraries
- opinionated, as I'm trying to solve problems I'm facing, not everyone's problems 😅

## Why use this?

You probably shouldn't 🤣. If possible, using more comprehensive libraries like Wolverine, NServiceBus, Brighter, etc, is probably a better idea. This toolkit is aimed at scenarios where greater degree of customization is wanted.
