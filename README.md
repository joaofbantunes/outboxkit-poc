# OutboxKit - Proof of Concept

This is a proof of concept of creating a toolkit to implement the outbox pattern.

The goal is not to provide a full implementation of an outbox, but to create foundations to implement it.

I'm going for something that is:

- somewhat generic - in order to accommodate different technologies, such as message brokers, databases and database access libraries
- opinionated - I'm trying to solve problems I'm facing, not everyone's problems 😅
- unambitious - again, I'm trying to solve my problems, not everyone's problems, so it fits my current needs in terms of features and performance, but might not fit yours

## Why use this?

You probably shouldn't 🤣. If possible, using more comprehensive libraries like Wolverine, NServiceBus, Brighter, etc, is probably a better idea. This toolkit is aimed at scenarios where greater degree of customization is wanted.
